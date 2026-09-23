use {
    crate::{grpc::E2EGeyserEventAdapter, scenarios::RunConfig},
    anyhow::{ensure, Context, Result},
    arc_swap::ArcSwap,
    futures::{channel::mpsc, StreamExt as FuturesStreamExt},
    solana_commitment_config::CommitmentLevel as MachineCommitment,
    std::{
        collections::{HashMap, HashSet},
        future::Future,
        pin::Pin,
        str::FromStr,
        sync::{Arc, Mutex},
        task::{Context as TaskContext, Poll},
        time::Duration,
    },
    yellowstone_block_machine::stream::{
        Block, BlockEventStore, BlockMachineOutput, BlockStream, SimpleBlockAccumulator,
        SimpleBlockStore,
    },
    yellowstone_grpc_client::{
        AutoReconnect, Backoff, ClientTlsConfig, DedupState, DedupStream, GrpcConnector,
        ReconnectConfig, ReconnectionPolicy, TonicGrpcConnector, DEFAULT_SLOT_RETENTION,
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

const SLOTS_AFTER_RECONNECT: usize = 20;
const OUTAGE: Duration = Duration::from_secs(4);
const TIMEOUT: Duration = Duration::from_secs(240);

#[derive(Default)]
struct ReconnectProgress {
    connected: bool,
    disconnects: usize,
    cursors: Vec<Option<u64>>,
    interrupted_slot: Option<u64>,
}

impl ReconnectProgress {
    const fn resumed(&self) -> bool {
        self.disconnects > 0 && !self.cursors.is_empty()
    }

    fn validate(&self, recover: bool) -> Result<()> {
        ensure!(
            self.resumed(),
            "no injected disconnect followed by a successful reconnect"
        );
        ensure!(
            self.cursors
                .iter()
                .all(|cursor| cursor.is_some() == recover),
            "unexpected reconnect replay cursors: {:?}; recover={recover}",
            self.cursors
        );
        log::info!(
            "verified {} injected disconnect(s), {} reconnect(s), cursors {:?}",
            self.disconnects,
            self.cursors.len(),
            self.cursors
        );
        Ok(())
    }
}

#[derive(Default)]
struct PartialSlot {
    transaction: bool,
    account: bool,
    entry: bool,
    created_bank: bool,
}

#[derive(Default)]
struct DisconnectReadiness {
    checkpoint: Option<u64>,
    saw_slot: bool,
    partial: HashMap<u64, PartialSlot>,
    require_entries: bool,
}

impl DisconnectReadiness {
    fn observe(&mut self, update: &SubscribeUpdate) -> Option<u64> {
        match update.update_oneof.as_ref()? {
            UpdateOneof::BlockMeta(meta) => {
                self.checkpoint = Some(
                    self.checkpoint
                        .map_or(meta.slot, |slot| slot.max(meta.slot)),
                );
                self.partial
                    .retain(|slot, _| *slot > self.checkpoint.unwrap());
            }
            UpdateOneof::Slot(slot) => {
                self.saw_slot = true;
                if slot.status == yellowstone_grpc_proto::geyser::SlotStatus::SlotCreatedBank as i32
                {
                    self.partial.entry(slot.slot).or_default().created_bank = true;
                }
            }
            UpdateOneof::Transaction(tx) if tx.transaction.is_some() => {
                self.partial.entry(tx.slot).or_default().transaction = true;
            }
            UpdateOneof::Account(account) if account.account.is_some() => {
                self.partial.entry(account.slot).or_default().account = true;
            }
            UpdateOneof::Entry(entry) => {
                self.partial.entry(entry.slot).or_default().entry = true;
            }
            _ => {}
        }
        let checkpoint = self.checkpoint?;
        if !self.saw_slot {
            return None;
        }
        self.partial.iter().find_map(|(&slot, data)| {
            (slot > checkpoint
                && data.transaction
                && data.account
                && (!self.require_entries || (data.entry && data.created_bank)))
                .then_some(slot)
        })
    }
}

struct DisconnectWhenReady<S> {
    inner: Option<S>,
    readiness: DisconnectReadiness,
    progress: Arc<Mutex<ReconnectProgress>>,
    pending_disconnect: Option<u64>,
    inject: bool,
}

impl<S> futures::Stream for DisconnectWhenReady<S>
where
    S: futures::Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
{
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(slot) = this.pending_disconnect.take() {
            this.inner.take();
            let mut progress = this.progress.lock().unwrap();
            progress.disconnects += 1;
            progress.interrupted_slot = Some(slot);
            log::info!(
                "injecting disconnect during partial slot {slot}, checkpoint {:?}",
                this.readiness.checkpoint
            );
            return Poll::Ready(Some(Err(Status::aborted("e2e: simulated disconnect"))));
        }
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let item = Pin::new(inner).poll_next(cx);
        if this.inject {
            if let Poll::Ready(Some(Ok(update))) = &item {
                this.pending_disconnect = this.readiness.observe(update);
            }
        }
        item
    }
}

#[derive(Clone)]
struct ScenarioConnector {
    inner: TonicGrpcConnector,
    progress: Arc<Mutex<ReconnectProgress>>,
}

impl GrpcConnector for ScenarioConnector {
    type Stream = Pin<Box<dyn futures::Stream<Item = Result<SubscribeUpdate, Status>> + Send>>;
    type ConnectError = <TonicGrpcConnector as GrpcConnector>::ConnectError;
    type ConnectFuture =
        Pin<Box<dyn Future<Output = Result<Self::Stream, Self::ConnectError>> + Send>>;

    fn connect(
        &self,
        request: Arc<SubscribeRequest>,
        from_slot: Option<u64>,
    ) -> Self::ConnectFuture {
        let inner = self.inner.clone();
        let progress = Arc::clone(&self.progress);
        Box::pin(async move {
            let reconnect = progress.lock().unwrap().connected;
            if reconnect {
                tokio::time::sleep(OUTAGE).await;
            }
            let require_entries = !request.entry.is_empty();
            let stream = inner.connect(request, from_slot).await?;
            {
                let mut state = progress.lock().unwrap();
                if reconnect {
                    state.cursors.push(from_slot);
                    log::info!("reconnected with from_slot={from_slot:?}");
                }
                state.connected = true;
            }

            let inject = progress.lock().unwrap().disconnects == 0;
            Ok(Box::pin(DisconnectWhenReady {
                inner: Some(stream),
                readiness: DisconnectReadiness {
                    require_entries,
                    ..Default::default()
                },
                progress,
                pending_disconnect: None,
                inject,
            }) as Self::Stream)
        })
    }
}

/// Slot, transaction, and account updates exercise replay and duplicate detection.
/// BlockMeta sets the replay checkpoint.
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
) -> Result<(ScenarioConnector, Arc<ArcSwap<SubscribeRequest>>)> {
    ensure!(
        config.dial.is_none(),
        "reconnect scenarios do not support --dial; use the target endpoint directly"
    );
    let mut endpoint =
        Endpoint::from_shared(config.endpoint.clone()).context("endpoint should be a valid URI")?;
    if config.endpoint.starts_with("https://") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_enabled_roots())?;
    }
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
        ScenarioConnector {
            inner,
            progress: Arc::new(Mutex::new(ReconnectProgress::default())),
        },
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
    slots: HashMap<u64, u64>,
    post_reconnect: HashSet<u64>,
    reconnect_gap: Option<(u64, u64)>,
    transactions: usize,
    accounts: usize,
}

impl Observed {
    /// Distinct slots, sorted.
    fn sorted_slots(&self) -> Vec<u64> {
        let mut slots: Vec<u64> = self.slots.keys().copied().collect();
        slots.sort_unstable();
        slots
    }

    fn gaps(&self) -> Vec<(u64, u64)> {
        self.sorted_slots()
            .windows(2)
            .filter(|w| self.slots[&w[1]] != w[0])
            .map(|w| (w[0], w[1]))
            .collect()
    }
}

/// Observe payloads and complete slots through an injected disconnect and reconnect.
async fn observe<S>(stream: &mut S, progress: &Arc<Mutex<ReconnectProgress>>) -> Result<Observed>
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
                let parent = m
                    .parent
                    .context("slot update must include parent for continuity checks")?;
                let previous_tip = observed.slots.keys().max().copied();
                if progress.lock().unwrap().resumed()
                    && previous_tip.is_some_and(|tip| m.slot > tip)
                {
                    if observed.post_reconnect.is_empty() {
                        let previous_tip = previous_tip.unwrap();
                        if parent > previous_tip {
                            observed.reconnect_gap = Some((previous_tip, m.slot));
                        }
                    }
                    observed.post_reconnect.insert(m.slot);
                }
                observed.slots.insert(m.slot, parent);
                Key::Slot(m.slot, m.status)
            }
            UpdateOneof::Transaction(m) => {
                let Some(tx) = m.transaction else { continue };
                if progress.lock().unwrap().resumed() {
                    observed.transactions += 1;
                }
                Key::Transaction(m.slot, tx.index)
            }
            UpdateOneof::Account(m) => {
                let Some(acct) = m.account else { continue };
                if progress.lock().unwrap().resumed() {
                    observed.accounts += 1;
                }
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

        if observed.post_reconnect.len() >= SLOTS_AFTER_RECONNECT {
            ensure!(
                observed.duplicates == 0,
                "{} duplicate events delivered",
                observed.duplicates
            );
            ensure!(
                observed.transactions > 0 && observed.accounts > 0,
                "expected post-reconnect transaction and account payloads, got {} transactions and {} accounts",
                observed.transactions,
                observed.accounts
            );
            return Ok(observed);
        }
    }

    anyhow::bail!("stream ended before observing {SLOTS_AFTER_RECONNECT} new slots after reconnect")
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

        let progress = Arc::clone(&connector.progress);
        let first = tokio::time::timeout(TIMEOUT, connector.connect(request.load_full(), None))
            .await
            .context("initial connection timed out")?
            .context("initial connection should succeed")?;

        let mut stream = DedupStream::new(
            AutoReconnect::new(first, connector, request, Backoff::default()),
            DedupState::with_slot_retention(DEFAULT_SLOT_RETENTION),
        );

        let observed = tokio::time::timeout(TIMEOUT, observe(&mut stream, &progress))
            .await
            .context("scenario timed out")??;

        progress.lock().unwrap().validate(true)?;
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

        let progress = Arc::clone(&connector.progress);
        let first = tokio::time::timeout(TIMEOUT, connector.connect(request.load_full(), None))
            .await
            .context("initial connection timed out")?
            .context("initial connection should succeed")?;

        let mut stream =
            AutoReconnect::new(first, connector, request, Backoff::default()).without_checkpoint();

        let observed = tokio::time::timeout(TIMEOUT, observe(&mut stream, &progress))
            .await
            .context("scenario timed out")??;

        progress.lock().unwrap().validate(false)?;
        ensure!(
            observed.reconnect_gap.is_some(),
            "skip policy at {commitment:?} did not skip a produced slot across the injected outage"
        );
    }

    Ok(())
}

/// Block reconstruction preserves blockhashes and event counts across reconnects.
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

    let progress = Arc::clone(&connector.progress);
    let first = tokio::time::timeout(TIMEOUT, connector.connect(request.load_full(), None))
        .await
        .context("initial connection timed out")?
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

    let mut metas: HashMap<u64, SubscribeUpdateBlockMeta> = HashMap::new();
    let mut built: HashMap<u64, Block<SimpleBlockStore<SubscribeUpdate>>> = HashMap::new();
    let mut rebuilt_after_reconnect = HashSet::new();
    let mut matched_after_reconnect = HashSet::new();

    tokio::time::timeout(TIMEOUT, async {
        while matched_after_reconnect.len() < BLOCKS_AFTER_RECONNECT
            || !progress
                .lock()
                .unwrap()
                .interrupted_slot
                .is_some_and(|slot| matched_after_reconnect.contains(&slot))
        {
            tokio::select! {
                meta = meta_stream.next() => {
                    let update = meta.context("block meta stream ended")??;
                    if let Some(UpdateOneof::BlockMeta(m)) = update.update_oneof {
                        let slot = m.slot;
                        ensure!(
                            metas.insert(slot, m).is_none(),
                            "block meta delivered twice for slot {slot}"
                        );
                        if rebuilt_after_reconnect.contains(&slot) {
                            matched_after_reconnect.insert(slot);
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
                            if progress.lock().unwrap().resumed() {
                                rebuilt_after_reconnect.insert(slot);
                                if metas.contains_key(&slot) {
                                    matched_after_reconnect.insert(slot);
                                }
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
            "timed out: {} of {BLOCKS_AFTER_RECONNECT} blocks rebuilt after reconnect",
            matched_after_reconnect.len()
        )
    })??;

    progress.lock().unwrap().validate(true)?;

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

        ensure!(
            meta.executed_transaction_count as usize == block.events.transaction_len(),
            "slot {slot}: expected {} transactions, got {}",
            meta.executed_transaction_count,
            block.events.transaction_len()
        );
        ensure!(
            meta.entries_count as usize == block.events.entry_len(),
            "slot {slot}: expected {} entries, got {}",
            meta.entries_count,
            block.events.entry_len()
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        yellowstone_grpc_proto::geyser::{
            SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateSlot,
            SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        },
    };

    fn slot(number: u64) -> Result<SubscribeUpdate, Status> {
        Ok(SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: number,
                parent: Some(number - 1),
                ..Default::default()
            })),
            ..Default::default()
        })
    }

    fn readiness_updates() -> Vec<SubscribeUpdate> {
        vec![
            SubscribeUpdate {
                update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                    slot: 10,
                    ..Default::default()
                })),
                ..Default::default()
            },
            slot(10).unwrap(),
            SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    slot: 11,
                    transaction: Some(SubscribeUpdateTransactionInfo::default()),
                    bank_id: 0,
                })),
                ..Default::default()
            },
            SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                    slot: 11,
                    account: Some(SubscribeUpdateAccountInfo::default()),
                    ..Default::default()
                })),
                ..Default::default()
            },
            SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 11,
                    parent: Some(10),
                    status: yellowstone_grpc_proto::geyser::SlotStatus::SlotCreatedBank as i32,
                    ..Default::default()
                })),
                ..Default::default()
            },
            SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Entry(
                    yellowstone_grpc_proto::geyser::SubscribeUpdateEntry {
                        slot: 11,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        ]
    }

    #[test]
    fn disconnect_requires_checkpoint_and_partial_payloads() {
        let updates = readiness_updates();
        let mut readiness = DisconnectReadiness::default();
        for update in &updates[..3] {
            assert_eq!(readiness.observe(update), None);
        }
        assert_eq!(readiness.observe(&updates[3]), Some(11));

        let mut readiness = DisconnectReadiness {
            require_entries: true,
            ..Default::default()
        };
        for update in &updates[..5] {
            assert_eq!(readiness.observe(update), None);
        }
        assert_eq!(readiness.observe(&updates[5]), Some(11));
        let sealed = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: 11,
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(readiness.observe(&sealed), None);
        assert_eq!(readiness.observe(&updates[5]), None);
    }

    #[tokio::test]
    async fn disconnect_delivers_partial_payload_then_drops_once() {
        let updates = readiness_updates();
        let progress = Arc::new(Mutex::new(ReconnectProgress::default()));
        let mut stream = DisconnectWhenReady {
            inner: Some(futures::stream::iter(updates.clone().into_iter().map(Ok))),
            readiness: DisconnectReadiness {
                require_entries: true,
                ..Default::default()
            },
            progress: Arc::clone(&progress),
            pending_disconnect: None,
            inject: true,
        };
        for expected in &updates {
            assert_eq!(&stream.next().await.unwrap().unwrap(), expected);
            assert_eq!(progress.lock().unwrap().disconnects, 0);
        }
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            yellowstone_grpc_proto::tonic::Code::Aborted
        );
        assert_eq!(progress.lock().unwrap().interrupted_slot, Some(11));
        assert!(stream.next().await.is_none());
        assert_eq!(progress.lock().unwrap().disconnects, 1);

        let mut resumed = DisconnectWhenReady {
            inner: Some(futures::stream::iter(updates.into_iter().map(Ok))),
            readiness: DisconnectReadiness::default(),
            progress: Arc::clone(&progress),
            pending_disconnect: None,
            inject: false,
        };
        while let Some(update) = resumed.next().await {
            assert!(update.is_ok());
        }
        assert_eq!(progress.lock().unwrap().disconnects, 1);
    }

    #[test]
    fn skipped_slot_is_not_missing_replay() {
        let mut observed = Observed::default();
        observed.slots.extend([(10, 9), (12, 10)]);
        assert!(observed.gaps().is_empty());
        observed.slots.insert(15, 14);
        assert_eq!(observed.gaps(), vec![(12, 15)]);
    }

    #[test]
    fn reconnect_requires_disconnect_and_correct_cursor() {
        let mut progress = ReconnectProgress::default();
        assert!(progress.validate(true).is_err());
        progress.disconnects = 1;
        assert!(progress.validate(true).is_err());
        progress.cursors.push(Some(10));
        assert!(progress.validate(true).is_ok());
        assert!(progress.validate(false).is_err());
        progress.cursors[0] = None;
        assert!(progress.validate(false).is_ok());
        assert!(progress.validate(true).is_err());
    }

    #[tokio::test]
    async fn collecting_slots_without_reconnect_cannot_pass() {
        let mut stream = futures::stream::iter((1..100).map(slot));
        let progress = Arc::new(Mutex::new(ReconnectProgress::default()));
        let error = observe(&mut stream, &progress).await.err().unwrap();
        assert!(error.to_string().contains("stream ended before"));
    }

    #[tokio::test]
    async fn post_reconnect_completion_checks_payloads_and_duplicates() {
        for duplicate in [false, true] {
            let progress = Arc::new(Mutex::new(ReconnectProgress {
                connected: true,
                disconnects: 1,
                cursors: vec![Some(1)],
                ..Default::default()
            }));
            let tx = SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    slot: 1,
                    transaction: Some(SubscribeUpdateTransactionInfo::default()),
                    bank_id: 0,
                })),
                ..Default::default()
            };
            let mut updates = vec![Ok(tx.clone())];
            if duplicate {
                updates.push(Ok(tx));
            }
            updates.push(Ok(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                    slot: 1,
                    account: Some(SubscribeUpdateAccountInfo::default()),
                    ..Default::default()
                })),
                ..Default::default()
            }));
            updates.extend((1..=SLOTS_AFTER_RECONNECT as u64 + 1).map(slot));
            let result = observe(&mut futures::stream::iter(updates), &progress).await;
            if duplicate {
                assert!(result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("duplicate events"));
            } else {
                assert_eq!(result.unwrap().post_reconnect.len(), SLOTS_AFTER_RECONNECT);
            }
        }
    }
}
