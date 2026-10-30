use {
    crate::scenarios::RunConfig,
    anyhow::{bail, ensure, Context, Result},
    arc_swap::ArcSwap,
    futures::{channel::mpsc, StreamExt as FuturesStreamExt},
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
        time::Duration,
    },
    yellowstone_grpc_client::{
        ClientTlsConfig, GrpcConnector, ReconnectConfig, ReconnectEvent, ReconnectGeyserStream,
        ReconnectPolicy, TonicGrpcConnector,
    },
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots, SubscribeUpdate,
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
// Every bank writes SlotHistory inside `freeze()`: after CreatedBank, before BlockMeta.
const SLOT_HISTORY: Pubkey = Pubkey::from_str_const("SysvarS1otHistory11111111111111111111111111");

#[derive(Default)]
struct ReconnectProgress {
    connected: bool,
    disconnects: usize,
    // The `from_slot` each reconnect asked for.
    reconnects: Vec<Option<u64>>,
    interrupted_slot: Option<u64>,
}

impl ReconnectProgress {
    const fn resumed(&self) -> bool {
        self.disconnects > 0 && !self.reconnects.is_empty()
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.resumed(),
            "no injected disconnect followed by a successful reconnect"
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

/// Cuts the stream right after the SlotHistory write of a bank with no block meta yet, once
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
                // Vote transactions are not a cut point: Alpenglow moves votes into the block footer.
                Some(UpdateOneof::Account(update))
                    if self.block_metas >= BLOCK_METAS_BEFORE_CUT
                        && update
                            .account
                            .as_ref()
                            .is_some_and(|account| account.pubkey == SLOT_HISTORY.as_ref())
                        && update.bank_id.is_some_and(|bank_id| {
                            !self.complete.contains(&(update.slot, bank_id))
                        }) =>
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
                    state.reconnects.push(from_slot);
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

/// Slot, transaction, and account updates exercise replay across a reconnect.
/// Slots take every status, so Finalized arrives and sets the replay start.
fn subscribe_request(commitment: CommitmentLevel) -> SubscribeRequest {
    SubscribeRequest {
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
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

fn connector(
    config: &RunConfig,
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

    // ReconnectGeyserStream swaps this sender on every reconnect; the scenario never
    // sends mid-stream requests, so the receiver is dropped.
    let (tx, _rx) = mpsc::channel(1000);

    let inner = TonicGrpcConnector::new(
        endpoint,
        ReconnectConfig::default(),
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

/// `Replay` resends from the slot after the last finalized one, so a forced disconnect
/// leaves no gap in the slots delivered.
#[test_helper(name = "reconnect-replay", tags = ["client", "reconnect"])]
pub async fn reconnect_should_replay_missed_slots(config: &RunConfig) -> Result<()> {
    futures::future::try_join(
        reconnect_at(config, CommitmentLevel::Processed, ReconnectPolicy::Replay),
        reconnect_at(config, CommitmentLevel::Confirmed, ReconnectPolicy::Replay),
    )
    .await?;
    Ok(())
}

/// `NoReplay` resumes at the live tip, so a forced disconnect leaves a gap.
#[test_helper(name = "reconnect-no-replay", tags = ["client", "reconnect"])]
pub async fn reconnect_should_skip_missed_slots(config: &RunConfig) -> Result<()> {
    futures::future::try_join(
        reconnect_at(
            config,
            CommitmentLevel::Processed,
            ReconnectPolicy::NoReplay,
        ),
        reconnect_at(
            config,
            CommitmentLevel::Confirmed,
            ReconnectPolicy::NoReplay,
        ),
    )
    .await?;
    Ok(())
}

async fn reconnect_at(
    config: &RunConfig,
    commitment: CommitmentLevel,
    policy: ReconnectPolicy,
) -> Result<()> {
    let (connector, request) = connector(config, commitment)?;

    let progress = Arc::clone(&connector.progress);
    let first = tokio::time::timeout(TIMEOUT, connector.connect(request.load_full(), None))
        .await
        .context("initial connection timed out")?
        .context("initial connection should succeed")?;

    // Only updates matter here; the `Missing` event is checked by `reconnect-rewind`.
    let mut stream = Box::pin(
        ReconnectGeyserStream::new(
            first,
            connector,
            request,
            ReconnectConfig::default().with_policy(policy),
        )
        .filter_map(|event| {
            futures::future::ready(match event {
                Ok(ReconnectEvent::Update(update)) => Some(Ok(update)),
                Ok(ReconnectEvent::Missing { .. }) => None,
                Err(status) => Some(Err(status)),
            })
        }),
    );

    let observed = tokio::time::timeout(TIMEOUT, observe(&mut stream, &progress))
        .await
        .context("scenario timed out")??;

    progress.lock().unwrap().validate()?;
    let replays = policy == ReconnectPolicy::Replay;
    ensure!(
        observed.reconnect_gap.is_none() == replays,
        "{policy:?} at {commitment:?}: gap across the injected outage {:?}",
        observed.reconnect_gap
    );

    Ok(())
}

/// Each reconnect emits `Missing { from_slot, to_slot }` before any new update, then replays
/// from the slot after the last finalized one.
#[test_helper(name = "reconnect-rewind", tags = ["client", "reconnect"])]
pub async fn reconnect_should_emit_rewind_before_new_stream(config: &RunConfig) -> Result<()> {
    let (connector, request) = connector(config, CommitmentLevel::Processed)?;
    // Only slots (every status, for Finalized), BlockMeta and sysvar writes (the cut point).
    let mut narrow = (*request.load_full()).clone();
    narrow.transactions.clear();
    narrow.slots = HashMap::from([(
        "test".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(false),
            interslot_updates: Some(false),
        },
    )]);
    narrow.accounts = HashMap::from([(
        "test".to_string(),
        yellowstone_grpc_proto::geyser::SubscribeRequestFilterAccounts {
            owner: vec!["Sysvar1111111111111111111111111111111111111".to_string()],
            ..Default::default()
        },
    )]);
    request.store(Arc::new(narrow));
    let progress = Arc::clone(&connector.progress);
    let first = connector.connect(request.load_full(), None).await?;
    let mut stream =
        ReconnectGeyserStream::new(first, connector, request, ReconnectConfig::default());

    tokio::time::timeout(TIMEOUT, async {
        let mut last_finalized_seen = None;
        let mut highest_slot_seen = None;
        while let Some(event) = stream.next().await {
            match event? {
                ReconnectEvent::Update(update) => {
                    highest_slot_seen = highest_slot_seen.max(slot_of(&update));
                    if let Some(UpdateOneof::Slot(slot)) = update.update_oneof {
                        if slot.status == yellowstone_grpc_proto::geyser::SlotStatus::SlotFinalized as i32 {
                            last_finalized_seen = last_finalized_seen.max(Some(slot.slot));
                        }
                    }
                }
                ReconnectEvent::Missing {
                    from_slot,
                    to_slot,
                } => {
                    let first_replayed_slot = first_slot_of_new_stream(&mut stream).await?;
                    // Only when piped (e.g. into jq): on a terminal it would break the spinners.
                    if !std::io::IsTerminal::is_terminal(&std::io::stdout()) {
                        println!(
                            "{}",
                            serde_json::json!({ "Missing": {
                                "from_slot": from_slot,
                                "to_slot": to_slot,
                            }})
                        );
                    }
                    let reconnects = progress.lock().unwrap().reconnects.clone();
                    ensure!(
                        reconnects == [from_slot.map(|slot| slot + 1)],
                        "expected one reconnect replaying after {from_slot:?}, got {reconnects:?}"
                    );
                    ensure!(
                        from_slot == last_finalized_seen,
                        "from_slot {from_slot:?} is not the last finalized slot seen {last_finalized_seen:?}"
                    );
                    ensure!(
                        Some(to_slot) == highest_slot_seen,
                        "to_slot {to_slot} is not the highest slot seen {highest_slot_seen:?}"
                    );
                    ensure!(
                        first_replayed_slot <= to_slot,
                        "replay starts at {first_replayed_slot}, past the range end {to_slot}"
                    );
                    return Ok(());
                }
            }
        }
        bail!("stream ended before a reconnect")
    })
    .await
    .context("scenario timed out")?
}

fn slot_of(update: &SubscribeUpdate) -> Option<u64> {
    match update.update_oneof.as_ref()? {
        UpdateOneof::Account(m) => Some(m.slot),
        UpdateOneof::Slot(m) => Some(m.slot),
        UpdateOneof::Transaction(m) => Some(m.slot),
        UpdateOneof::BlockMeta(m) => Some(m.slot),
        _ => None,
    }
}

async fn first_slot_of_new_stream<S>(stream: &mut S) -> Result<u64>
where
    S: futures::Stream<Item = Result<ReconnectEvent, Status>> + Unpin,
{
    while let Some(event) = stream.next().await {
        let ReconnectEvent::Update(update) = event? else {
            bail!("two Missing events in a row");
        };
        if let Some(slot) = slot_of(&update) {
            return Ok(slot);
        }
    }
    bail!("new stream ended before its first update")
}
