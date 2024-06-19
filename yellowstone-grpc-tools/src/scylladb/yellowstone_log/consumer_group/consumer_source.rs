use {
    super::{
        context::ConsumerContext,
        lock::{ConsumerLock, FencingTokenGenerator},
        shard_iterator::{ShardFilter, ShardIterator},
    },
    crate::scylladb::{
        scylladb_utils::LwtResult,
        types::{
            BlockchainEvent, BlockchainEventType, ConsumerGroupId, ConsumerId, ProducerId, ShardId,
            ShardOffsetMap, Slot, UNDEFINED_SLOT,
        },
    },
    core::fmt,
    futures::{future::try_join_all, Future, FutureExt},
    scylla::{
        batch::{Batch, BatchType},
        prepared_statement::PreparedStatement,
        Session,
    },
    std::{collections::BTreeMap, convert::identity, sync::Arc, time::Duration},
    thiserror::Error,
    tokio::{
        sync::{
            mpsc::{self, error::SendError},
            oneshot,
        },
        task::{JoinError, JoinHandle},
        time::Instant,
    },
    tracing::{info, warn},
};

const CLIENT_LAG_WARN_THRESHOLD: Duration = Duration::from_millis(250);

const DEFAULT_OFFSET_COMMIT_INTERVAL: Duration = Duration::from_millis(500);

const FETCH_MICRO_BATCH_LATENCY_WARN_THRESHOLD: Duration = Duration::from_millis(500);

const UPDATE_CONSUMER_SHARD_OFFSET_V2: &str = r###"
    UPDATE consumer_shard_offset_v2
    SET 
        acc_shard_offset_map = ?, 
        tx_shard_offset_map = ?, 
        revision = ?
    WHERE
        consumer_group_id = ?
        AND consumer_id = ?
        AND producer_id = ?
    IF revision < ?
"###;

pub struct ConsumerSource<T: FromBlockchainEvent> {
    ctx: ConsumerContext,
    sender: mpsc::Sender<T>,
    // The interval at which we want to commit our Offset progression to Scylla
    offset_commit_interval: Duration,
    shard_iterators: BTreeMap<ShardId, ShardIterator>,
    update_consumer_shard_offset_v2_ps: PreparedStatement,
}

#[derive(Clone, Debug, PartialEq, Error, Eq, Copy)]
pub struct Interrupted;

impl fmt::Display for Interrupted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Interrupted")
    }
}

pub trait FromBlockchainEvent: Send + 'static {
    fn from(blockchain_event: BlockchainEvent) -> Self;
}

impl FromBlockchainEvent for BlockchainEvent {
    fn from(blockchain_event: BlockchainEvent) -> Self {
        blockchain_event
    }
}
pub struct ConsumerSourceHandle {
    // This is public to ease integration test
    pub tx: oneshot::Sender<()>,
    pub handle: JoinHandle<anyhow::Result<()>>,
}

impl ConsumerSourceHandle {
    pub async fn gracefully_shutdown(self) -> anyhow::Result<()> {
        if let Err(e) = self.tx.send(()) {
            warn!("failed to send interrupt signal to consumer source: {e:?}");
        }
        let result = self.handle.await?;
        result
    }
}

impl Future for ConsumerSourceHandle {
    type Output = anyhow::Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.handle
            .poll_unpin(cx)
            .map(|join_result| join_result.map_err(anyhow::Error::new).and_then(identity))
    }
}

impl<T: FromBlockchainEvent> ConsumerSource<T> {
    pub async fn new(
        ctx: ConsumerContext,
        shard_offset_map_per_blockchain_event_type: BTreeMap<BlockchainEventType, ShardOffsetMap>,
        sender: mpsc::Sender<T>,
        offset_commit_interval: Option<Duration>,
        filter: Option<ShardFilter>,
    ) -> anyhow::Result<Self> {
        let mut shard_iterators = try_join_all(
            shard_offset_map_per_blockchain_event_type
                .into_iter()
                .flat_map(|(ev_type, shard_offset_map)| {
                    shard_offset_map
                        .into_iter()
                        .map(move |(k, v)| (ev_type, k, v))
                })
                .map(|(ev_type, shard_id, (offset, slot))| {
                    ShardIterator::new(
                        ctx.session(),
                        ctx.producer_id,
                        shard_id,
                        offset,
                        slot,
                        ev_type,
                        filter.clone(),
                    )
                }),
        )
        .await?;

        let update_consumer_shard_offset_v2_ps = ctx
            .session()
            .prepare(UPDATE_CONSUMER_SHARD_OFFSET_V2)
            .await?;
        // Prewarm every shard iterator
        try_join_all(shard_iterators.iter_mut().map(|shard_it| shard_it.warm())).await?;

        Ok(ConsumerSource {
            ctx,
            sender,
            offset_commit_interval: offset_commit_interval
                .unwrap_or(DEFAULT_OFFSET_COMMIT_INTERVAL),
            shard_iterators: shard_iterators
                .into_iter()
                .map(|shard_it| (shard_it.shard_id, shard_it))
                .collect(),
            update_consumer_shard_offset_v2_ps,
        })
    }

    fn get_shard_offset_map(&self, ev_type: BlockchainEventType) -> ShardOffsetMap {
        self.shard_iterators
            .iter()
            .filter(|(_, v)| v.event_type == ev_type)
            .map(|(k, v)| {
                let slot = v.last_slot;
                (*k, (v.last_offset(), slot))
            })
            .collect()
    }

    async fn update_consumer_shard_offsets_v2(&self) -> anyhow::Result<()> {
        let b1 = self
            .ctx
            .subscribed_event_types
            .contains(&BlockchainEventType::AccountUpdate);
        let b2 = self
            .ctx
            .subscribed_event_types
            .contains(&BlockchainEventType::NewTransaction);
        let (acc_shard_offsets, tx_shard_offsets) = match (b1, b2) {
            (true, false) => {
                let map = self.get_shard_offset_map(BlockchainEventType::AccountUpdate);
                (map.clone(), map)
            }
            (false, true) => {
                let map = self.get_shard_offset_map(BlockchainEventType::NewTransaction);
                (map.clone(), map)
            }
            (true, true) => {
                let map1 = self.get_shard_offset_map(BlockchainEventType::AccountUpdate);
                let map2 = self.get_shard_offset_map(BlockchainEventType::NewTransaction);
                (map1, map2)
            }
            (false, false) => panic!("no blockchain event subscribed to"),
        };
        let revision = self.ctx.generate_fencing_token().await?;
        let values = (
            acc_shard_offsets,
            tx_shard_offsets,
            revision,
            &self.ctx.consumer_group_id,
            &self.ctx.consumer_id,
            &self.ctx.producer_id,
            revision,
        );
        let lwt_result = self
            .ctx
            .session()
            .execute(&self.update_consumer_shard_offset_v2_ps, values)
            .await?
            .first_row_typed::<LwtResult>()?;
        if let LwtResult(false) = lwt_result {
            anyhow::bail!("Failed to update shard offset, lock is compromised");
        }
        Ok(())
    }

    pub fn spawn(mut self) -> ConsumerSourceHandle {
        info!("spawn consumer source");
        let (tx, rx) = oneshot::channel();

        let handle = tokio::spawn(async move { self.run(rx).await });

        ConsumerSourceHandle { tx, handle }
    }

    pub async fn run(&mut self, mut rx: oneshot::Receiver<()>) -> anyhow::Result<()> {
        let consumer_id = self.ctx.consumer_id.to_owned();
        let mut commit_offset_deadline = Instant::now() + self.offset_commit_interval;
        const PRINT_CONSUMER_SLOT_REACH_DELAY: Duration = Duration::from_secs(5);

        let mut max_seen_slot = UNDEFINED_SLOT;
        let mut num_event_between_two_slots = 0;

        let mut next_trace_schedule = Instant::now() + PRINT_CONSUMER_SLOT_REACH_DELAY;
        let mut t = Instant::now();
        loop {
            for (shard_id, shard_it) in self.shard_iterators.iter_mut() {
                match rx.try_recv() {
                    Err(oneshot::error::TryRecvError::Empty) => (),
                    _ => {
                        warn!("consumer {consumer_id} received an interrupted signal");
                        //self.update_consumer_shard_offsets().await?;
                        self.update_consumer_shard_offsets_v2().await?;
                        return Ok(());
                    }
                }

                let maybe = shard_it.try_next().await?;

                if let Some(block_chain_event) = maybe {
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
                    tokio::select! {
                        _ = &mut rx => {
                            warn!("consumer {consumer_id} received an interrupt signal while sending blockchain event");
                            //self.update_consumer_shard_offsets().await?;
                            self.update_consumer_shard_offsets_v2().await?;
                            return Ok(());
                        }
                        result = self.sender.send(T::from(block_chain_event)) => {
                            if let Err(SendError(_)) = result {
                                anyhow::bail!("consumer {consumer_id} closed its streaming half");
                            }
                        }
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
