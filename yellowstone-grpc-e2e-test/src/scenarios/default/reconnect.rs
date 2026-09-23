use {
    crate::{
        grpc::E2EGeyserEventAdapter,
        reconnect_blocks::{ReconnectBlockEvent, ReconnectBlockStream},
        scenarios::RunConfig,
    },
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
        time::Duration,
    },
    yellowstone_block_machine::stream::{
        Block, BlockEventStore, BlockMachineOutput, BlockStream, SimpleBlockAccumulator,
        SimpleBlockStore,
    },
    yellowstone_grpc_client::{
        test_tools::Unstable, AutoReconnect, Backoff, BankRef, ClientTlsConfig, DedupState,
        DedupStream, DiscardReason, GrpcConnector, ReconnectConfig, ReconnectEvent,
        ReconnectStream, ReconnectionPolicy, SlotWinner, TonicGrpcConnector,
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

const SLOTS_AFTER_RECONNECT: usize = 20;
const DISCONNECT_AFTER: Duration = Duration::from_secs(4);
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

fn observe_disconnect<S>(
    stream: S,
    progress: Arc<Mutex<ReconnectProgress>>,
) -> impl futures::Stream<Item = Result<SubscribeUpdate, Status>>
where
    S: futures::Stream<Item = Result<SubscribeUpdate, Status>>,
{
    let mut accounts = HashSet::new();
    let mut complete = HashSet::new();
    let mut recorded = false;
    stream.inspect(move |item| match item {
        Ok(update) => match update.update_oneof.as_ref() {
            Some(UpdateOneof::Account(account)) if account.account.is_some() => {
                accounts.insert((account.slot, account.bank_id));
            }
            Some(UpdateOneof::BlockMeta(meta)) => {
                complete.insert((meta.slot, Some(meta.bank_id)));
            }
            _ => {}
        },
        Err(error)
            if !recorded
                && error.code() == yellowstone_grpc_proto::tonic::Code::Aborted
                && error.message() == "unstable: simulated disconnect" =>
        {
            recorded = true;
            let mut state = progress.lock().unwrap();
            state.disconnects += 1;
            state.interrupted_slot = accounts
                .iter()
                .filter(|bank| !complete.contains(bank))
                .map(|(slot, _)| *slot)
                .max();
            log::info!(
                "timed disconnect: interrupted_partial_slot={:?}",
                state.interrupted_slot
            );
        }
        _ => {}
    })
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
            let stream = inner.connect(request, from_slot).await?;
            {
                let mut state = progress.lock().unwrap();
                if reconnect {
                    state.cursors.push(from_slot);
                    log::info!("reconnected with from_slot={from_slot:?}");
                }
                state.connected = true;
            }

            if !reconnect {
                Ok(Box::pin(observe_disconnect(
                    Unstable::new(stream, DISCONNECT_AFTER),
                    progress,
                )) as Self::Stream)
            } else {
                Ok(Box::pin(stream) as Self::Stream)
            }
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

    #[tokio::test]
    async fn unstable_disconnect_does_not_wait_for_partial_payloads() {
        let progress = Arc::new(Mutex::new(ReconnectProgress::default()));
        let source = futures::stream::iter(vec![slot(10)]);
        let mut stream =
            observe_disconnect(Unstable::new(source, Duration::ZERO), Arc::clone(&progress));
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            yellowstone_grpc_proto::tonic::Code::Aborted
        );
        let state = progress.lock().unwrap();
        assert_eq!(state.disconnects, 1);
        assert_eq!(state.interrupted_slot, None);
    }

    #[tokio::test]
    async fn timed_disconnect_records_only_unfinished_account_banks() {
        let account = |bank_id| {
            Ok(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                    slot: 11,
                    bank_id: Some(bank_id),
                    account: Some(SubscribeUpdateAccountInfo::default()),
                    ..Default::default()
                })),
                ..Default::default()
            })
        };
        for partial in [false, true] {
            let progress = Arc::new(Mutex::new(ReconnectProgress::default()));
            let mut updates = vec![
                account(7),
                Ok(SubscribeUpdate {
                    update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                        slot: 11,
                        bank_id: 7,
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
            ];
            if partial {
                updates.push(account(8));
            }
            updates.push(Err(Status::aborted("unstable: simulated disconnect")));
            let mut stream =
                observe_disconnect(futures::stream::iter(updates), Arc::clone(&progress));
            while stream.next().await.is_some() {}
            let state = progress.lock().unwrap();
            assert_eq!(state.disconnects, 1);
            assert_eq!(state.interrupted_slot, partial.then_some(11));
        }
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
                    ..Default::default()
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

/// Partial-bank disconnect, finalized replacement, and continued delivery.
#[test_helper(name = "reconnect-bank-recovery", tags = ["client", "reconnect"])]
pub async fn reconnect_should_replace_partial_bank(config: &RunConfig) -> Result<()> {
    let (connector, request) = connector(
        config,
        ReconnectionPolicy::RecoverMissedData {
            slot_retention: DEFAULT_SLOT_RETENTION,
        },
        CommitmentLevel::Processed,
    )?;

    // Recovery needs finalized statuses even though payloads are processed.
    let mut subscription = subscribe_request(CommitmentLevel::Processed);
    for filter in subscription.slots.values_mut() {
        filter.filter_by_commitment = Some(false);
    }
    request.store(Arc::new(subscription));

    let progress = Arc::clone(&connector.progress);
    let first = tokio::time::timeout(TIMEOUT, connector.connect(request.load_full(), None))
        .await
        .context("initial connection timed out")??;

    // Bank recovery buffers replacement data until it can identify the winning bank.
    let mut stream = ReconnectStream::new(
        AutoReconnect::new(first, connector, request, Backoff::default())
            .with_bank_replay_for_test(),
    );

    tokio::time::timeout(TIMEOUT, async {
        let mut original_accounts = HashSet::<BankRef>::new();
        let mut original_complete = HashSet::<BankRef>::new();
        let mut replacement_generation = None;
        let mut interrupted_slot = None;
        let mut winner_hash = None::<String>;
        let mut replacement_accounts = HashMap::<u64, usize>::new();
        let mut replacement_complete = false;
        let mut later_slots = HashSet::new();

        while let Some(event) = stream.next().await {
            match event.context("bank recovery failed")? {
                ReconnectEvent::DiscardBanks { banks, reason, replacement, winners } => {
                    ensure!(replacement_generation.is_none(), "unexpected additional recovery");
                    ensure!(reason == DiscardReason::IncompleteDelivery, "unexpected discard reason");
                    let slot = progress.lock().unwrap().interrupted_slot
                        .context("timed disconnect did not interrupt a delivered partial account bank")?;

                    // A slot can contain several banks; check each delivered partial bank.
                    let partial_banks: Vec<_> = original_accounts.iter()
                        .filter(|bank| bank.slot == slot && !original_complete.contains(*bank))
                        .copied().collect();
                    ensure!(!partial_banks.is_empty(), "disconnect did not interrupt a delivered partial bank");
                    ensure!(partial_banks.iter().all(|bank| banks.contains(bank)),
                        "discard omitted an interrupted partial bank");
                    ensure!(banks.iter().all(|bank| bank.generation == 0 && bank.slot >= replacement.from_slot),
                        "discard contains banks outside the old generation/replay range");
                    ensure!(replacement.generation > 0 && replacement.from_slot <= slot,
                        "invalid replacement generation or boundary");

                    let hash = winners.iter().find_map(|winner| match winner {
                        SlotWinner::Finalized { slot: winner_slot, blockhash } if *winner_slot == slot => Some(blockhash.clone()),
                        _ => None,
                    });
                    let hash = hash.context(
                        "interrupted slot has no finalized winner; \
                         this replacement-payload test cannot pass on a skipped slot",
                    )?;
                    log::info!(
                        "DISCARD: banks={banks:?}, replacement={replacement:?}, \
                         interrupted_slot={slot}, winner={hash}"
                    );
                    interrupted_slot = Some(slot);
                    winner_hash = Some(hash);
                    replacement_generation = Some(replacement.generation);
                }
                ReconnectEvent::Update { generation, update } => {
                    let Some(payload) = update.update_oneof else { continue };
                    if generation == 0 {
                        ensure!(replacement_generation.is_none(), "old-generation update leaked after discard");
                        match payload {
                            UpdateOneof::Account(account) => {
                                let bank_id = account.bank_id.context("endpoint does not supply account bank IDs")?;
                                original_accounts.insert(BankRef { generation, slot: account.slot, bank_id });
                            }
                            UpdateOneof::BlockMeta(meta) => {
                                original_complete.insert(BankRef { generation, slot: meta.slot, bank_id: meta.bank_id });
                            }
                            _ => {}
                        }
                        continue;
                    }
                    ensure!(replacement_generation == Some(generation),
                        "replacement update arrived before its discard event");
                    let slot = interrupted_slot.unwrap();
                    match payload {
                        UpdateOneof::Account(account) if account.slot == slot => {
                            let bank_id = account.bank_id.context("replacement account has no bank ID")?;
                            ensure!(account.account.is_some(), "replacement account has no payload");
                            *replacement_accounts.entry(bank_id).or_default() += 1;
                        }
                        UpdateOneof::BlockMeta(meta) if meta.slot == slot
                            && Some(meta.blockhash.as_str()) == winner_hash.as_deref() => {
                            let count = replacement_accounts.get(&meta.bank_id).copied().unwrap_or_default();
                            ensure!(count > 0,
                                "winning bank's BlockMeta arrived without preceding replacement account data");
                            replacement_complete = true;
                            log::info!(
                                "REPLACEMENT: generation={generation}, slot={slot}, \
                                 bank={}, account_updates={count}, BlockMeta received", meta.bank_id
                            );
                        }
                        UpdateOneof::Slot(status) if replacement_complete && status.slot > slot => {
                            later_slots.insert(status.slot);
                        }
                        _ => {}
                    }
                    if later_slots.len() >= SLOTS_AFTER_RECONNECT {
                        progress.lock().unwrap().validate(true)?;
                        log::info!(
                            "PASS: partial bank discarded, winning replacement \
                             delivered, then {} later slots observed", later_slots.len()
                        );
                        return Ok(());
                    }
                }
            }
        }
        anyhow::bail!("stream ended before recovery verification completed")
    })
    .await
    .context("bank recovery scenario timed out")?
}

fn verify_block_integrity(
    rebuilt: &Block<SimpleBlockStore<SubscribeUpdate>>,
    reference: &yellowstone_grpc_proto::geyser::SubscribeUpdateBlock,
) -> Result<()> {
    ensure!(rebuilt.slot == reference.slot, "block slot mismatch");
    ensure!(
        rebuilt.blockhash == solana_hash::Hash::from_str(&reference.blockhash)?.to_bytes(),
        "rebuilt blockhash differs from finalized reference"
    );
    let mut transactions = HashMap::new();
    let mut accounts = HashMap::new();
    let mut entries = HashMap::new();
    for event in rebuilt.events.iter() {
        match event.update_oneof.as_ref() {
            Some(UpdateOneof::Transaction(tx)) => {
                ensure!(
                    tx.slot == reference.slot && tx.bank_id == reference.bank_id,
                    "transaction from wrong bank"
                );
                let info = tx
                    .transaction
                    .as_ref()
                    .context("transaction payload missing")?;
                ensure!(
                    transactions.insert(info.index, info).is_none(),
                    "duplicate transaction index"
                );
            }
            Some(UpdateOneof::Account(account)) => {
                ensure!(
                    account.slot == reference.slot && account.bank_id == Some(reference.bank_id),
                    "account from wrong bank"
                );
                let info = account
                    .account
                    .as_ref()
                    .context("account payload missing")?;
                let previous = accounts.entry(info.pubkey.clone()).or_insert(info);
                if info.write_version > previous.write_version {
                    *previous = info;
                }
            }
            Some(UpdateOneof::Entry(entry)) => {
                ensure!(
                    entry.slot == reference.slot && entry.bank_id == reference.bank_id,
                    "entry from wrong bank"
                );
                ensure!(
                    entries.insert(entry.index, entry).is_none(),
                    "duplicate entry index"
                );
            }
            _ => {}
        }
    }
    ensure!(
        transactions.len() as u64 == reference.executed_transaction_count
            && transactions.len() == reference.transactions.len(),
        "transaction count mismatch"
    );
    ensure!(
        entries.len() as u64 == reference.entries_count && entries.len() == reference.entries.len(),
        "entry count mismatch"
    );
    ensure!(
        accounts.len() as u64 == reference.updated_account_count
            && accounts.len() == reference.accounts.len(),
        "account count mismatch"
    );
    for tx in &reference.transactions {
        ensure!(
            transactions.remove(&tx.index) == Some(tx),
            "transaction {} payload or metadata mismatch",
            tx.index
        );
    }
    for entry in &reference.entries {
        ensure!(
            entries.remove(&entry.index) == Some(entry),
            "entry {} hash or transaction range mismatch",
            entry.index
        );
    }
    for account in &reference.accounts {
        ensure!(
            accounts.remove(&account.pubkey) == Some(account),
            "final account payload mismatch"
        );
    }
    log::info!(
        "INTEGRITY: slot={}, transactions={}, entries={}, accounts={} match finalized full block",
        reference.slot,
        reference.transactions.len(),
        reference.entries.len(),
        reference.accounts.len()
    );
    Ok(())
}

/// Rebuilds the interrupted bank and compares its payloads with a finalized full block.
#[test_helper(name = "reconnect-bank-integrity", tags = ["client", "reconnect", "blockmachine"])]
pub async fn reconnect_should_preserve_block_integrity(config: &RunConfig) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        let mut client = crate::grpc::new_client(config).await?;
        let mut reference_stream = client.subscribe_once(SubscribeRequest {
            blocks: HashMap::from([("integrity".to_owned(), yellowstone_grpc_proto::geyser::SubscribeRequestFilterBlocks {
                include_transactions: Some(true),
                include_accounts: Some(true),
                include_entries: Some(true),
                ..Default::default()
            })]),
            commitment: Some(CommitmentLevel::Finalized as i32),
            ..Default::default()
        }).await?;
        let (connector, request) = connector(config,
            ReconnectionPolicy::RecoverMissedData { slot_retention: DEFAULT_SLOT_RETENTION },
            CommitmentLevel::Processed)?;
        let mut subscription = block_machine_request();
        for filter in subscription.slots.values_mut() {
            filter.filter_by_commitment = Some(false);
        }
        request.store(Arc::new(subscription));
        let progress = Arc::clone(&connector.progress);
        let first = connector.connect(request.load_full(), None).await?;
        let stream = ReconnectStream::new(
            AutoReconnect::new(first, connector, request, Backoff::default()).with_bank_replay_for_test());
        let mut blocks = ReconnectBlockStream::new(stream);
        let mut winning_hash = None;
        let mut rebuilt: Option<(u64, Block<SimpleBlockStore<SubscribeUpdate>>)> = None;
        let mut references = HashMap::new();
        loop {
            tokio::select! {
                event = blocks.next() => {
                    match event.context("block recovery stream ended")?? {
                        ReconnectBlockEvent::DiscardBanks { banks, reason, replacement, winners } => {
                            log::info!("INTEGRITY: discard reason={reason:?}, replacement={replacement:?}, banks={banks:?}");
                            let slot = progress.lock().unwrap().interrupted_slot.context("timed disconnect did not interrupt a delivered partial account bank")?;
                            if let Some(hash) = winners.iter().find_map(|winner| match winner {
                                SlotWinner::Finalized { slot: s, blockhash } if *s == slot => Some(blockhash.clone()),
                                _ => None,
                            }) {
                                winning_hash = Some(hash);
                            }
                            if rebuilt.as_ref().is_some_and(|(generation, block)| {
                                banks.iter().any(|bank| bank.generation == *generation && bank.slot == block.slot)
                            }) {
                                rebuilt = None;
                            }
                        }
                        ReconnectBlockEvent::Output { generation, output } => match output {
                            BlockMachineOutput::FrozenBlock(block) => {
                                log::info!("INTEGRITY OUTPUT: FrozenBlock generation={generation}, slot={}, blockhash={}",
                                    block.slot, solana_hash::Hash::new_from_array(block.blockhash));
                                if generation > 0 && Some(block.slot) == progress.lock().unwrap().interrupted_slot {
                                    log::info!("INTEGRITY: rebuilt interrupted slot {}", block.slot);
                                    rebuilt = Some((generation, block));
                                }
                            }
                            BlockMachineOutput::DeadBlockDetected(dead) => log::warn!(
                                "INTEGRITY OUTPUT: DeadBlockDetected generation={generation}, slot={}", dead.slot),
                            BlockMachineOutput::ForkDetected(fork) => log::warn!(
                                "INTEGRITY OUTPUT: ForkDetected generation={generation}, slot={}", fork.slot),
                            BlockMachineOutput::SlotCommitmentUpdate(status) => log::info!(
                                "INTEGRITY OUTPUT: SlotCommitmentUpdate generation={generation}, status={status:?}"),
                        }
                    }
                }
                update = reference_stream.next() => {
                    let update = update.context("reference stream ended")??;
                    if let Some(UpdateOneof::Block(block)) = update.update_oneof {
                        let target = progress.lock().unwrap().interrupted_slot;
                        // The reference can arrive before the timed disconnect identifies the target slot.
                        if target.is_none() || target == Some(block.slot) {
                            log::info!("INTEGRITY: received finalized reference for slot {}", block.slot);
                            references.insert(block.slot, block);
                        }
                    }
                }
            }
            let target = progress.lock().unwrap().interrupted_slot;
            if let Some(slot) = target {
                references.retain(|reference_slot, _| *reference_slot == slot);
            }
            let reference = target.and_then(|slot| references.get(&slot));
            if let (Some((_, rebuilt)), Some(reference), Some(hash)) = (&rebuilt, reference, &winning_hash) {
                ensure!(&reference.blockhash == hash, "reference differs from recovery winner");
                // Processed forks can freeze before the finalized bank arrives.
                if rebuilt.blockhash != solana_hash::Hash::from_str(hash)?.to_bytes() {
                    continue;
                }
                verify_block_integrity(rebuilt, reference)?;
                ensure!(progress.lock().unwrap().resumed(),
                    "no injected disconnect followed by a successful reconnect");
                return Ok(());
            }
        }
    }).await.context("bank integrity scenario timed out")?
}

#[cfg(test)]
mod integrity_tests {
    use {super::*, yellowstone_grpc_proto::geyser::*};

    #[test]
    fn integrity_accepts_matching_empty_block() {
        let reference = SubscribeUpdateBlock {
            slot: 10,
            blockhash: solana_hash::Hash::default().to_string(),
            ..Default::default()
        };
        let rebuilt = Block {
            slot: 10,
            blockhash: [0; 32],
            events: SimpleBlockStore {
                slot: 10,
                events: vec![],
                account_idx_map: vec![],
                transaction_idx_map: vec![],
                entry_idx_map: vec![],
                other_idx_map: vec![],
            },
        };
        verify_block_integrity(&rebuilt, &reference).unwrap();
    }

    #[test]
    fn integrity_rejects_corruption_with_unchanged_counts() {
        let account = SubscribeUpdateAccountInfo {
            pubkey: vec![1; 32],
            lamports: 42,
            ..Default::default()
        };
        let transaction = SubscribeUpdateTransactionInfo {
            signature: vec![2; 64],
            ..Default::default()
        };
        let entry = SubscribeUpdateEntry {
            slot: 10,
            bank_id: 7,
            hash: vec![3; 32],
            executed_transaction_count: 1,
            ..Default::default()
        };
        let reference = SubscribeUpdateBlock {
            slot: 10,
            bank_id: 7,
            blockhash: solana_hash::Hash::default().to_string(),
            executed_transaction_count: 1,
            updated_account_count: 1,
            entries_count: 1,
            accounts: vec![account.clone()],
            transactions: vec![transaction.clone()],
            entries: vec![entry.clone()],
            ..Default::default()
        };
        let events = vec![
            UpdateOneof::Account(SubscribeUpdateAccount {
                slot: 10,
                bank_id: Some(7),
                account: Some(account),
                ..Default::default()
            }),
            UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot: 10,
                bank_id: 7,
                transaction: Some(transaction),
            }),
            UpdateOneof::Entry(entry),
        ]
        .into_iter()
        .map(|payload| SubscribeUpdate {
            update_oneof: Some(payload),
            ..Default::default()
        })
        .collect();
        let rebuilt = Block {
            slot: 10,
            blockhash: [0; 32],
            events: SimpleBlockStore {
                slot: 10,
                events,
                account_idx_map: vec![0],
                transaction_idx_map: vec![1],
                entry_idx_map: vec![2],
                other_idx_map: vec![],
            },
        };
        verify_block_integrity(&rebuilt, &reference).unwrap();
        let mut corrupt = reference.clone();
        corrupt.accounts[0].lamports += 1;
        assert!(verify_block_integrity(&rebuilt, &corrupt)
            .unwrap_err()
            .to_string()
            .contains("account payload"));
        let mut corrupt = reference.clone();
        corrupt.transactions[0].signature[0] ^= 1;
        assert!(verify_block_integrity(&rebuilt, &corrupt)
            .unwrap_err()
            .to_string()
            .contains("payload or metadata"));
        let mut corrupt = reference.clone();
        corrupt.entries[0].hash[0] ^= 1;
        assert!(verify_block_integrity(&rebuilt, &corrupt)
            .unwrap_err()
            .to_string()
            .contains("hash or transaction range"));
        let mut corrupt = reference;
        corrupt.bank_id += 1;
        assert!(verify_block_integrity(&rebuilt, &corrupt)
            .unwrap_err()
            .to_string()
            .contains("wrong bank"));
    }
}
