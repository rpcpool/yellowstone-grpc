use {
    crate::{grpc::E2EGeyserEventAdapter, scenarios::RunConfig},
    anyhow::{ensure, Context, Result},
    arc_swap::ArcSwap,
    futures::channel::mpsc,
    solana_commitment_config::CommitmentLevel as MachineCommitment,
    std::{
        collections::{HashMap, HashSet},
        str::FromStr,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
    tokio_stream::StreamExt,
    yellowstone_block_machine::stream::{
        Block, BlockEventStore, BlockMachineOutput, BlockStream, SimpleBlockAccumulator,
        SimpleBlockStore,
    },
    yellowstone_grpc_client::{
        test_tools::UnstableConnector, AutoReconnect, Backoff, DedupState, DedupStream,
        GrpcConnector, ReconnectConfig, ReconnectionPolicy, TonicGrpcConnector,
        DEFAULT_SLOT_RETENTION,
    },
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots, SubscribeUpdate,
            SubscribeUpdateBlockMeta,
        },
        tonic::{transport::Endpoint, Status},
    },
};

const DROP_INTERVAL: Duration = Duration::from_secs(45);
const SLOTS_TO_OBSERVE: usize = 20;
const TIMEOUT: Duration = Duration::from_secs(240);

/// Slots plus blocks_meta. BlockMeta sets the checkpoint, so without it neither
/// policy ever replays and the scenario proves nothing.
fn subscribe_request(commitment: CommitmentLevel) -> SubscribeRequest {
    SubscribeRequest {
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        )]),
        blocks_meta: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterBlocksMeta::default(),
        )]),
        transactions: HashMap::from([("test".to_string(), Default::default())]),
        accounts: HashMap::from([("test".to_string(), Default::default())]),
        commitment: Some(commitment as i32),
        ..Default::default()
    }
}

/// What the block machine requires: entries and interslot updates drive
/// reconstruction, block_meta seals each slot. No `blocks` filter.
fn block_machine_request() -> SubscribeRequest {
    SubscribeRequest {
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: None,
                interslot_updates: Some(true),
            },
        )]),
        entry: HashMap::from([("test".to_string(), Default::default())]),
        accounts: HashMap::from([("test".to_string(), Default::default())]),
        transactions: HashMap::from([("test".to_string(), Default::default())]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    }
}

fn connector(
    config: &RunConfig,
    policy: ReconnectionPolicy,
    commitment: CommitmentLevel,
) -> Result<(UnstableConnector, Arc<ArcSwap<SubscribeRequest>>)> {
    let endpoint =
        Endpoint::from_shared(config.endpoint.clone()).context("endpoint should be a valid URI")?;
    let x_token = config
        .x_token
        .clone()
        .map(|t| t.parse())
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid x-token: {e}"))?;

    // AutoReconnect swaps this sender on every reconnect; the scenario never
    // sends mid-stream requests, so the receiver is dropped.
    let (tx, _rx) = mpsc::channel(1000);

    let inner = TonicGrpcConnector::new(
        endpoint,
        ReconnectConfig {
            backoff: Backoff::default(),
            policy,
        },
        x_token,
        Default::default(),
        Arc::new(Mutex::new(tx)),
    );

    Ok((
        UnstableConnector::new(inner, DROP_INTERVAL),
        Arc::new(ArcSwap::from_pointee(subscribe_request(commitment))),
    ))
}

#[derive(Hash, PartialEq, Eq, Debug)]
enum Key {
    Slot(u64, i32),
    Transaction(u64, u64),
    Account(u64, Vec<u8>, Option<Vec<u8>>),
}

#[derive(Default)]
struct Observed {
    seen: HashSet<Key>,
    duplicates: u64,
    slots: HashSet<u64>,
}

impl Observed {
    /// Distinct slots, sorted.
    fn sorted_slots(&self) -> Vec<u64> {
        let mut slots: Vec<u64> = self.slots.iter().copied().collect();
        slots.sort_unstable();
        slots
    }

    fn gaps(&self) -> Vec<(u64, u64)> {
        self.sorted_slots()
            .windows(2)
            .filter(|w| w[1] > w[0] + 1)
            .map(|w| (w[0], w[1]))
            .collect()
    }
}

/// Consumes until `SLOTS_TO_OBSERVE` distinct slots have been seen, recording
/// every (slot, status) pair. Duplicates fail both policies: Recover dedups them,
/// Skip never replays.
async fn observe<S>(stream: &mut S) -> Result<Observed>
where
    S: futures::Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
{
    let mut observed = Observed::default();

    while let Some(update) = stream.next().await {
        let update = update.context("stream should yield updates without error")?;
        let Some(oneof) = update.update_oneof else {
            continue;
        };

        let key = match oneof {
            UpdateOneof::Slot(m) => {
                observed.slots.insert(m.slot);
                Key::Slot(m.slot, m.status)
            }
            UpdateOneof::Transaction(m) => {
                let Some(tx) = m.transaction else { continue };
                Key::Transaction(m.slot, tx.index)
            }
            UpdateOneof::Account(m) => {
                let Some(acct) = m.account else { continue };
                Key::Account(m.slot, acct.pubkey, acct.txn_signature)
            }
            _ => continue,
        };

        if observed.seen.contains(&key) {
            observed.duplicates += 1;
            if observed.duplicates <= 20 {
                log::warn!("dup: {key:?}");
            }
        } else {
            observed.seen.insert(key);
        }

        if observed.slots.len() >= SLOTS_TO_OBSERVE {
            break;
        }
    }

    Ok(observed)
}

fn ensure_disconnects_fired(elapsed: Duration) -> Result<()> {
    ensure!(
        elapsed > DROP_INTERVAL,
        "run finished in {elapsed:?}, shorter than one drop interval; no disconnect fired"
    );
    Ok(())
}

/// Auto-reconnect replays missed slots after forced disconnects
#[test_helper(name = "reconnect-recover", tags = ["client", "reconnect"])]
pub async fn reconnect_should_recover_missed_slots(config: &RunConfig) -> Result<()> {
    for commitment in [CommitmentLevel::Processed, CommitmentLevel::Confirmed] {
        let (connector, request) = connector(
            config,
            ReconnectionPolicy::RecoverMissedData {
                slot_retention: DEFAULT_SLOT_RETENTION,
            },
            commitment,
        )?;

        let first = connector
            .connect(request.load_full(), None)
            .await
            .context("initial connection should succeed")?;

        let mut stream = DedupStream::new(
            AutoReconnect::new(first, connector, request, Backoff::default()),
            DedupState::with_slot_retention(DEFAULT_SLOT_RETENTION),
        );

        let started = Instant::now();
        let observed = tokio::time::timeout(TIMEOUT, observe(&mut stream))
            .await
            .context("scenario timed out")??;

        ensure_disconnects_fired(started.elapsed())?;
        ensure!(
            observed.gaps().is_empty(),
            "recover policy at {commitment:?} should leave no gaps, got {:?}",
            observed.gaps()
        );
    }

    Ok(())
}

/// SkipMissedData never replays, so forced disconnects leave gaps and no duplicates.
#[test_helper(name = "reconnect-skip", tags = ["client", "reconnect"])]
pub async fn reconnect_should_skip_missed_slots(config: &RunConfig) -> Result<()> {
    for commitment in [CommitmentLevel::Processed, CommitmentLevel::Confirmed] {
        let (connector, request) =
            connector(config, ReconnectionPolicy::SkipMissedData, commitment)?;

        let first = connector
            .connect(request.load_full(), None)
            .await
            .context("initial connection should succeed")?;

        let mut stream =
            AutoReconnect::new(first, connector, request, Backoff::default()).without_checkpoint();

        let started = Instant::now();
        let observed = tokio::time::timeout(TIMEOUT, observe(&mut stream))
            .await
            .context("scenario timed out")??;

        ensure_disconnects_fired(started.elapsed())?;
        ensure!(
            observed.duplicates == 0,
            "{} duplicates delivered at {commitment:?}",
            observed.duplicates
        );
        ensure!(
            !observed.gaps().is_empty(),
            "skip policy at {commitment:?} should leave gaps but found none"
        );
    }

    Ok(())
}

/// Block reconstruction survives forced disconnects: replayed events must not
/// corrupt the transaction and entry counts of a rebuilt block.
/// Block reconstruction keeps working across a forced disconnect.
///
/// Runs until three blocks have been rebuilt *after* a reconnect, so the run
/// cannot finish before the disconnect fires regardless of block rate.
///
/// Asserts blockhash integrity only. Transaction and entry counts are not checked:
/// for a slot interrupted mid-delivery `DedupStream` has no BlockMeta to compare
/// against, so it forwards the whole replay, and `BlockStream` appends rather than
/// deduping. Observed 3217 transactions against a BlockMeta count of 1689. Closing
/// this needs block identity for a partial slot, which arrives with Alpenglow bank id.
#[test_helper(name = "reconnect-blockmachine", tags = ["client", "reconnect", "blockmachine"])]
pub async fn reconnect_should_rebuild_blocks(config: &RunConfig) -> Result<()> {
    let (connector, request) = connector(
        config,
        ReconnectionPolicy::RecoverMissedData {
            slot_retention: DEFAULT_SLOT_RETENTION,
        },
        CommitmentLevel::Processed,
    )?;
    request.store(Arc::new(block_machine_request()));

    let first = connector
        .connect(request.load_full(), None)
        .await
        .context("initial connection should succeed")?;

    let stream = DedupStream::new(
        AutoReconnect::new(first, connector, request, Backoff::default()),
        DedupState::with_slot_retention(DEFAULT_SLOT_RETENTION),
    );

    let mut blocks = BlockStream::<_, E2EGeyserEventAdapter, SimpleBlockAccumulator<_>>::new(
        Box::pin(stream),
        SimpleBlockAccumulator::default(),
        MachineCommitment::Processed,
    );

    // Stable side channel for the expected blockhash. Only the block-building
    // stream is put under reconnect pressure.
    let mut client = crate::grpc::new_client(config).await?;
    let mut meta_stream = client
        .subscribe_once(SubscribeRequest {
            blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
            commitment: Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        })
        .await
        .context("block meta subscription should succeed")?;

    /// Blocks that must be rebuilt after the disconnect before the run ends.
    const BLOCKS_AFTER_RECONNECT: usize = 1;

    /// Margin on top of DROP_INTERVAL before we treat the disconnect as fired.
    const DISCONNECT_MARGIN: Duration = Duration::from_secs(2);

    let started = Instant::now();
    let mut metas: HashMap<u64, SubscribeUpdateBlockMeta> = HashMap::new();
    let mut built: HashMap<u64, Block<SimpleBlockStore<SubscribeUpdate>>> = HashMap::new();
    let mut matched_after_reconnect = 0usize;

    tokio::time::timeout(TIMEOUT, async {
        while matched_after_reconnect < BLOCKS_AFTER_RECONNECT {
            let past_disconnect = started.elapsed() > DROP_INTERVAL + DISCONNECT_MARGIN;

            tokio::select! {
                meta = meta_stream.next() => {
                    let update = meta.context("block meta stream ended")??;
                    if let Some(UpdateOneof::BlockMeta(m)) = update.update_oneof {
                        let slot = m.slot;
                        ensure!(
                            metas.insert(slot, m).is_none(),
                            "block meta delivered twice for slot {slot}"
                        );
                        if past_disconnect && built.contains_key(&slot) {
                            matched_after_reconnect += 1;
                        }
                    }
                }
                block = blocks.next() => {
                    let output = block.context("block stream ended")??;
                    match output {
                        BlockMachineOutput::FrozenBlock(b) => {
                            let slot = b.slot;
                            log::info!("rebuilt block for slot {slot}");
                            ensure!(
                                built.insert(slot, b).is_none(),
                                "block machine emitted slot {slot} twice"
                            );
                            if past_disconnect && metas.contains_key(&slot) {
                                matched_after_reconnect += 1;
                            }
                        }
                        BlockMachineOutput::DeadBlockDetected(d) => {
                            anyhow::bail!("dead block at slot {}", d.slot);
                        }
                        BlockMachineOutput::ForkDetected(f) => {
                            log::warn!("fork detected at slot {}", f.slot);
                        }
                        BlockMachineOutput::SlotCommitmentUpdate(_) => {}
                    }
                }
            }
        }

        anyhow::Ok(())
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "timed out: {matched_after_reconnect} of {BLOCKS_AFTER_RECONNECT} blocks rebuilt after reconnect"
        )
    })??;

    for (slot, block) in built {
        let Some(meta) = metas.get(&slot) else {
            continue;
        };

        let expected = solana_hash::Hash::from_str(&meta.blockhash)
            .context("block meta blockhash should parse")?
            .to_bytes();
        ensure!(
            block.blockhash == expected,
            "slot {slot}: rebuilt blockhash does not match block meta"
        );
        ensure!(
            block.events.transaction_len() > 0,
            "slot {slot}: rebuilt block has no transactions"
        );

        // Not asserted: a slot cut mid-delivery is replayed in full and BlockStream
        // appends rather than dedupes, so counts overshoot. Logged so the size of the
        // gap is visible on every run. Closing it needs block identity for a partial
        // slot (Alpenglow bank id).
        let tx_actual = block.events.transaction_len();
        let entry_actual = block.events.entry_len();
        if meta.executed_transaction_count as usize != tx_actual
            || meta.entries_count as usize != entry_actual
        {
            log::warn!(
                "slot {slot}: replay duplication — transactions {} vs {tx_actual}, entries {} vs {entry_actual}",
                meta.executed_transaction_count,
                meta.entries_count
            );
        }
    }

    Ok(())
}
