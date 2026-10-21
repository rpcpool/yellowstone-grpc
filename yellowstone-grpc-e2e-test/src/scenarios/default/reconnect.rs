use {
    crate::{
        grpc::BlockEventCounts,
        reconnect_blocks::{ReconnectBlockEvent, ReconnectBlockStream},
        scenarios::RunConfig,
    },
    anyhow::{ensure, Context, Result},
    arc_swap::ArcSwap,
    futures::{channel::mpsc, StreamExt as FuturesStreamExt},
    std::{
        collections::{HashMap, HashSet},
        future::Future,
        pin::Pin,
        str::FromStr,
        sync::{Arc, Mutex},
        time::Duration,
    },
    yellowstone_block_machine::stream::{
        Block, BlockEventStore, BlockMachineOutput, SimpleBlockStore,
    },
    yellowstone_grpc_client::{
        AutoReconnect, Backoff, BankRef, ClientTlsConfig, DedupState, DedupStream, DiscardReason,
        GrpcConnector, ReconnectConfig, ReconnectEvent, ReconnectStream, ReconnectionPolicy,
        SlotWinner, TonicGrpcConnector, DEFAULT_SLOT_RETENTION,
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

// Covers the ~5 slots produced during the outage plus a few live slots after it.
const SLOTS_AFTER_RECONNECT: usize = 12;
// Block metas the first connection must deliver before the cut, so a replay checkpoint exists.
const BLOCK_METAS_BEFORE_CUT: usize = 3;
const OUTAGE: Duration = Duration::from_secs(2);
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

/// Cuts the stream right after a transaction of a bank with no block meta yet, once
/// `BLOCK_METAS_BEFORE_CUT` block metas arrived. The cut always lands mid-bank, after a checkpoint.
struct CutMidBank<S> {
    inner: Option<S>,
    block_metas: usize,
    complete: HashSet<(u64, u64)>,
    cut_next: bool,
}

impl<S> CutMidBank<S> {
    fn new(inner: S) -> Self {
        Self {
            inner: Some(inner),
            block_metas: 0,
            complete: HashSet::new(),
            cut_next: false,
        }
    }
}

impl<S> futures::Stream for CutMidBank<S>
where
    S: futures::Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
{
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.cut_next {
            self.cut_next = false;
            self.inner = None;
            return std::task::Poll::Ready(Some(Err(Status::aborted(
                "unstable: simulated disconnect",
            ))));
        }
        let Some(inner) = self.inner.as_mut() else {
            return std::task::Poll::Ready(None);
        };
        let item = std::task::ready!(inner.poll_next_unpin(cx));
        if let Some(Ok(update)) = &item {
            match update.update_oneof.as_ref() {
                Some(UpdateOneof::BlockMeta(meta)) => {
                    self.block_metas += 1;
                    self.complete.insert((meta.slot, meta.bank_id));
                }
                // A transaction comes after its bank's CreatedBank, unlike construction-time
                // sysvar writes, so the cut never lands before the bank is opened.
                Some(UpdateOneof::Transaction(tx))
                    if self.block_metas >= BLOCK_METAS_BEFORE_CUT
                        && !self.complete.contains(&(tx.slot, tx.bank_id)) =>
                {
                    self.cut_next = true;
                }
                _ => {}
            }
        }
        std::task::Poll::Ready(item)
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
                Ok(Box::pin(observe_disconnect(CutMidBank::new(stream), progress)) as Self::Stream)
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

/// Every bank writes sysvars and has vote transactions: enough payloads to cut and rebuild a
/// bank, and a fraction of the full firehose a replay would otherwise resend.
fn narrow_payloads(subscription: &mut SubscribeRequest) {
    subscription.accounts = HashMap::from([(
        "test".to_string(),
        yellowstone_grpc_proto::geyser::SubscribeRequestFilterAccounts {
            owner: vec!["Sysvar1111111111111111111111111111111111111".to_string()],
            ..Default::default()
        },
    )]);
    subscription.transactions = HashMap::from([(
        "test".to_string(),
        yellowstone_grpc_proto::geyser::SubscribeRequestFilterTransactions {
            vote: Some(true),
            ..Default::default()
        },
    )]);
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
    // write_version tells apart writes without a transaction signature: on Alpenglow the
    // Clock sysvar is written twice per slot, at bank creation and at the block footer.
    Account(u64, Vec<u8>, Option<Vec<u8>>, u64),
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
                Key::Account(m.slot, acct.pubkey, acct.txn_signature, acct.write_version)
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
    futures::future::try_join(
        recover_missed_slots_at(config, CommitmentLevel::Processed),
        recover_missed_slots_at(config, CommitmentLevel::Confirmed),
    )
    .await?;
    Ok(())
}

async fn recover_missed_slots_at(config: &RunConfig, commitment: CommitmentLevel) -> Result<()> {
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

    Ok(())
}

/// SkipMissedData never replays, so forced disconnects leave gaps and no duplicates.
#[test_helper(name = "reconnect-skip", tags = ["client", "reconnect"])]
pub async fn reconnect_should_skip_missed_slots(config: &RunConfig) -> Result<()> {
    futures::future::try_join(
        skip_missed_slots_at(config, CommitmentLevel::Processed),
        skip_missed_slots_at(config, CommitmentLevel::Confirmed),
    )
    .await?;
    Ok(())
}

async fn skip_missed_slots_at(config: &RunConfig, commitment: CommitmentLevel) -> Result<()> {
    let (connector, request) = connector(config, ReconnectionPolicy::SkipMissedData, commitment)?;

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
    // Bank recovery needs finalized statuses to pick the winner of a discarded bank.
    let mut subscription = block_machine_request();
    for filter in subscription.slots.values_mut() {
        filter.filter_by_commitment = Some(false);
    }
    request.store(Arc::new(subscription));

    // Stable side channel for the expected blockhash, opened first so it sees every block
    // the block-building stream sees. Only the block-building stream is put under reconnect pressure.
    let mut client = crate::grpc::new_client(config).await?;
    let mut meta_stream = client
        .subscribe_once(SubscribeRequest {
            blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
            commitment: Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        })
        .await
        .context("block meta subscription should succeed")?;

    let progress = Arc::clone(&connector.progress);
    let first = tokio::time::timeout(TIMEOUT, connector.connect(request.load_full(), None))
        .await
        .context("initial connection timed out")?
        .context("initial connection should succeed")?;

    // A partially delivered bank is discarded and its replacement rebuilt, never repaired.
    let stream = ReconnectStream::new(
        AutoReconnect::new(first, connector, request, Backoff::default())
            .with_bank_replay_for_test(),
    );
    let mut blocks = ReconnectBlockStream::new(stream);

    /// Blocks that must be rebuilt after the disconnect before the run ends.
    const BLOCKS_AFTER_RECONNECT: usize = 1;

    // Keyed by (slot, bank_id): a slot can have more than one bank, each with its own block.
    let mut metas: HashMap<(u64, u64), SubscribeUpdateBlockMeta> = HashMap::new();
    let mut built: HashMap<(u64, u64), Block<SimpleBlockStore<SubscribeUpdate>>> = HashMap::new();
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
                        let bank_id = m.bank_id;
                        ensure!(
                            metas.insert((slot, bank_id), m).is_none(),
                            "block meta delivered twice for slot {slot} bank {bank_id}"
                        );
                        if rebuilt_after_reconnect.contains(&slot) {
                            matched_after_reconnect.insert(slot);
                        }
                    }
                }
                event = blocks.next() => {
                    let output = match event.context("block stream ended")?? {
                        ReconnectBlockEvent::DiscardBanks { banks, .. } => {
                            for bank in banks {
                                built.remove(&(bank.slot, bank.bank_id));
                            }
                            continue;
                        }
                        ReconnectBlockEvent::Output { output, .. } => output,
                    };
                    match output {
                        BlockMachineOutput::FrozenBlock(b) => {
                            let slot = b.slot;
                            let bank_id = b.bank_id;
                            log::info!("rebuilt block for slot {slot} bank {bank_id}");
                            ensure!(
                                built.insert((slot, bank_id), b).is_none(),
                                "block machine emitted slot {slot} bank {bank_id} twice"
                            );
                            if progress.lock().unwrap().resumed() {
                                rebuilt_after_reconnect.insert(slot);
                                if metas.contains_key(&(slot, bank_id)) {
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
                        BlockMachineOutput::BankDiscarded(discarded) => {
                            log::info!(
                                "bank {} of slot {} discarded",
                                discarded.bank_id,
                                discarded.slot
                            );
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

    for ((slot, bank_id), block) in built {
        let Some(meta) = metas.get(&(slot, bank_id)) else {
            continue;
        };
        let counts = BlockEventCounts::of(&block.events);

        let expected = solana_hash::Hash::from_str(&meta.blockhash)
            .context("block meta blockhash should parse")?
            .to_bytes();
        ensure!(
            block.blockhash == expected,
            "slot {slot}: rebuilt blockhash does not match block meta"
        );
        ensure!(
            counts.transactions > 0,
            "slot {slot}: rebuilt block has no transactions"
        );

        ensure!(
            meta.executed_transaction_count as usize == counts.transactions,
            "slot {slot}: expected {} transactions, got {}",
            meta.executed_transaction_count,
            counts.transactions
        );
        ensure!(
            meta.entries_count as usize == counts.entries,
            "slot {slot}: expected {} entries, got {}",
            meta.entries_count,
            counts.entries
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

    fn meta(slot: u64) -> Result<SubscribeUpdate, Status> {
        Ok(SubscribeUpdate {
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                bank_id: slot,
                ..Default::default()
            })),
            ..Default::default()
        })
    }

    fn account_of(slot: u64) -> Result<SubscribeUpdate, Status> {
        Ok(SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot,
                bank_id: Some(slot),
                account: Some(SubscribeUpdateAccountInfo::default()),
                ..Default::default()
            })),
            ..Default::default()
        })
    }

    fn tx_of(slot: u64) -> Result<SubscribeUpdate, Status> {
        Ok(SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                slot,
                bank_id: slot,
                transaction: Some(SubscribeUpdateTransactionInfo::default()),
            })),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn cut_lands_mid_bank_after_enough_block_metas() {
        let mut updates = vec![tx_of(1), meta(1), meta(2)];
        updates.extend([
            tx_of(3),
            meta(3),
            tx_of(3),
            account_of(4),
            tx_of(4),
            tx_of(4),
        ]);
        let mut stream = CutMidBank::new(futures::stream::iter(updates));
        let mut delivered = Vec::new();
        let error = loop {
            match stream.next().await.expect("the cut should end the stream") {
                Ok(update) => delivered.push(update),
                Err(error) => break error,
            }
        };
        assert_eq!(error.code(), yellowstone_grpc_proto::tonic::Code::Aborted);
        // Slot 3's late transaction belongs to a complete bank. Slot 4's account comes before its
        // CreatedBank, so only slot 4's first transaction triggers the cut.
        assert_eq!(delivered.len(), 8);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn unstable_disconnect_does_not_wait_for_partial_payloads() {
        let progress = Arc::new(Mutex::new(ReconnectProgress::default()));
        let source = futures::stream::iter(vec![slot(10)]);
        let mut stream = observe_disconnect(
            yellowstone_grpc_client::test_tools::Unstable::new(source, Duration::ZERO),
            Arc::clone(&progress),
        );
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
    narrow_payloads(&mut subscription);
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

/// Rebuilds the interrupted bank with the block machine and checks it is the finalized winner.
#[test_helper(name = "reconnect-bank-integrity", tags = ["client", "reconnect", "blockmachine"])]
pub async fn reconnect_should_preserve_block_integrity(config: &RunConfig) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        let (connector, request) = connector(
            config,
            ReconnectionPolicy::RecoverMissedData {
                slot_retention: DEFAULT_SLOT_RETENTION,
            },
            CommitmentLevel::Processed,
        )?;
        let mut subscription = block_machine_request();
        for filter in subscription.slots.values_mut() {
            filter.filter_by_commitment = Some(false);
        }
        narrow_payloads(&mut subscription);
        request.store(Arc::new(subscription));
        let progress = Arc::clone(&connector.progress);
        let first = connector.connect(request.load_full(), None).await?;
        let stream = ReconnectStream::new(
            AutoReconnect::new(first, connector, request, Backoff::default())
                .with_bank_replay_for_test(),
        );
        let mut blocks = ReconnectBlockStream::new(stream);
        let mut winning_hash = None;
        let mut rebuilt: Option<Block<SimpleBlockStore<SubscribeUpdate>>> = None;
        loop {
            match blocks
                .next()
                .await
                .context("block recovery stream ended")??
            {
                ReconnectBlockEvent::DiscardBanks { banks, winners, .. } => {
                    let slot = progress
                        .lock()
                        .unwrap()
                        .interrupted_slot
                        .context("disconnect did not interrupt a delivered partial bank")?;
                    winning_hash = winners.iter().find_map(|winner| match winner {
                        SlotWinner::Finalized { slot: s, blockhash } if *s == slot => {
                            Some(blockhash.clone())
                        }
                        _ => None,
                    });
                    if rebuilt
                        .as_ref()
                        .is_some_and(|block| banks.iter().any(|bank| bank.slot == block.slot))
                    {
                        rebuilt = None;
                    }
                }
                ReconnectBlockEvent::Output {
                    generation,
                    output: BlockMachineOutput::FrozenBlock(block),
                } if generation > 0
                    && Some(block.slot) == progress.lock().unwrap().interrupted_slot =>
                {
                    rebuilt = Some(block);
                }
                ReconnectBlockEvent::Output { .. } => {}
            }
            let (Some(block), Some(hash)) = (&rebuilt, &winning_hash) else {
                continue;
            };
            // Processed forks can freeze before the finalized bank arrives.
            if block.blockhash != solana_hash::Hash::from_str(hash)?.to_bytes() {
                continue;
            }
            let entries: HashSet<u64> = block
                .events
                .iter()
                .filter_map(|event| match event.update_oneof.as_ref() {
                    Some(UpdateOneof::Entry(entry)) => Some(entry.index),
                    _ => None,
                })
                .collect();
            ensure!(
                entries == (0..block.entry_count).collect(),
                "slot {}: rebuilt bank has entries {entries:?}, expected 0..{}",
                block.slot,
                block.entry_count
            );
            ensure!(
                progress.lock().unwrap().resumed(),
                "no injected disconnect followed by a successful reconnect"
            );
            log::info!(
                "INTEGRITY: slot {} rebuilt as the finalized winner with all {} entries",
                block.slot,
                block.entry_count
            );
            return Ok(());
        }
    })
    .await
    .context("bank integrity scenario timed out")?
}
