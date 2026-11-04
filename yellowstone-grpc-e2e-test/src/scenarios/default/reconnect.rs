//! Reconnect scenarios for [`ReconnectStream`].
//!
//! A connector cuts the first connection in the middle of a bank, after its SlotHistory write
//! and before its block meta. The scenario then checks what the stream reports in its
//! [`Disconnected`] event, and what it delivers on the new connection.

use {
    crate::{grpc::new_client, scenarios::RunConfig},
    anyhow::{bail, ensure, Context, Result},
    futures::{SinkExt, Stream, StreamExt},
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{ready, Poll},
        time::Duration,
    },
    yellowstone_grpc_client::{
        BankRef, ClientTlsConfig, Disconnected, GrpcConnector, ReconnectConfig, ReconnectEvent,
        ReconnectPolicy, ReconnectRequest, ReconnectStream, TonicGrpcConnector,
        RECONNECT_FILTER_KEY,
    },
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SlotStatus, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlockFooter,
            SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterSlots, SubscribeUpdate,
        },
        tonic::{transport::Endpoint, Status},
    },
};

/// Block metas the first connection delivers before the cut, so a confirmed slot is known.
const BLOCK_METAS_BEFORE_CUT: usize = 3;
/// Banks past the cut the new connection must deliver before a scenario checks the stream.
const BANKS_AFTER_RECONNECT: usize = 10;
const TIMEOUT: Duration = Duration::from_secs(240);
const FILTER: &str = "test";
/// Every bank writes SlotHistory inside `freeze()`: after its transactions, before its block
/// meta. Cutting right after it always lands mid-bank.
const SLOT_HISTORY: Pubkey = Pubkey::from_str_const("SysvarS1otHistory11111111111111111111111111");
const SYSVAR_OWNER: &str = "Sysvar1111111111111111111111111111111111111";

/// Sealing updates a sealing-only subscription gets before the cut.
const SEALED_BEFORE_CUT: usize = 5;

/// An update the server sends after all of a bank's content: what a sealing-only subscription
/// gets, one per bank.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sealing {
    Block,
    BlockFooter,
    BlockMeta,
}

impl Sealing {
    /// A subscription to this update alone, at `commitment`.
    fn request(self, commitment: CommitmentLevel) -> SubscribeRequest {
        let mut request = SubscribeRequest {
            commitment: Some(commitment as i32),
            ..Default::default()
        };
        match self {
            Self::Block => {
                request.blocks.insert(
                    FILTER.to_owned(),
                    SubscribeRequestFilterBlocks {
                        include_transactions: Some(false),
                        include_accounts: Some(false),
                        include_entries: Some(false),
                        ..Default::default()
                    },
                );
            }
            Self::BlockFooter => {
                request.block_footer.insert(
                    FILTER.to_owned(),
                    SubscribeRequestFilterBlockFooter::default(),
                );
            }
            Self::BlockMeta => {
                request.blocks_meta.insert(
                    FILTER.to_owned(),
                    SubscribeRequestFilterBlocksMeta::default(),
                );
            }
        }
        request
    }

    /// The bank and parent slot of an update of this kind. A footer carries no parent slot.
    fn bank_of(self, update: &SubscribeUpdate) -> Option<(BankRef, Option<u64>)> {
        let bank = |slot, bank_id| BankRef { slot, bank_id };
        match (self, update.update_oneof.as_ref()?) {
            (Self::Block, UpdateOneof::Block(m)) => {
                Some((bank(m.slot, m.bank_id), Some(m.parent_slot)))
            }
            (Self::BlockFooter, UpdateOneof::BlockFooter(m)) => {
                Some((bank(m.slot, m.bank_id), None))
            }
            (Self::BlockMeta, UpdateOneof::BlockMeta(m)) => {
                Some((bank(m.slot, m.bank_id), Some(m.parent_slot)))
            }
            _ => None,
        }
    }
}

/// Where [`Cut`] ends the first connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CutPoint {
    /// Right after the SlotHistory write of a bank whose block meta has not arrived, once
    /// [`BLOCK_METAS_BEFORE_CUT`] block metas arrived.
    MidBank,
    /// Right after the [`SEALED_BEFORE_CUT`]th distinct update of this kind. For a Block or a
    /// footer, that is before the bank's block meta.
    After(Sealing),
    /// Right after the CreatedBank status of a bank whose block meta has not arrived, once
    /// [`BLOCK_METAS_BEFORE_CUT`] block metas arrived. Agave writes the bank's SlotHistory at
    /// freeze, after CreatedBank, so at processed the cut lands before the bank's last account
    /// write.
    AfterCreatedBank,
}

/// What the [`CuttingConnector`] saw, shared with the scenario.
#[derive(Debug, Default)]
struct ConnectLog {
    /// The `from_slot` of every connect, in order.
    from_slots: Vec<Option<u64>>,
    /// The bank the first connection was cut in, or after with [`CutPoint::After`].
    cut: Option<BankRef>,
}

/// Ends a connection at its [`CutPoint`].
struct Cut<S> {
    inner: Option<S>,
    point: CutPoint,
    log: Arc<Mutex<ConnectLog>>,
    block_metas: usize,
    complete: HashSet<BankRef>,
    sealed: HashSet<BankRef>,
    cut_next: bool,
}

impl<S> Cut<S> {
    fn new(inner: S, point: CutPoint, log: Arc<Mutex<ConnectLog>>) -> Self {
        Self {
            inner: Some(inner),
            point,
            log,
            block_metas: 0,
            complete: HashSet::new(),
            sealed: HashSet::new(),
            cut_next: false,
        }
    }

    /// Cuts the connection before its next update.
    fn cut_after(&mut self, bank: BankRef) {
        self.cut_next = true;
        self.log.lock().unwrap().cut = Some(bank);
        log::info!(
            "cutting the first connection at bank {bank:?} ({:?})",
            self.point
        );
    }
}

impl<S> Stream for Cut<S>
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
{
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.cut_next {
            self.cut_next = false;
            self.inner = None;
            return Poll::Ready(Some(Err(Status::aborted("e2e: cut"))));
        }
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let item = ready!(inner.poll_next_unpin(cx));
        if let Some(Ok(update)) = &item {
            if let CutPoint::After(sealing) = self.point {
                // The user's and the reconnect filter's copies can come as two updates.
                if let Some((bank, _)) = sealing.bank_of(update) {
                    if self.sealed.insert(bank) && self.sealed.len() == SEALED_BEFORE_CUT {
                        self.cut_after(bank);
                    }
                }
            }
            match update.update_oneof.as_ref() {
                Some(UpdateOneof::BlockMeta(meta)) => {
                    self.block_metas += 1;
                    self.complete.insert(BankRef {
                        slot: meta.slot,
                        bank_id: meta.bank_id,
                    });
                }
                Some(UpdateOneof::Slot(status))
                    if self.point == CutPoint::AfterCreatedBank
                        && self.block_metas >= BLOCK_METAS_BEFORE_CUT
                        && status.status == SlotStatus::SlotCreatedBank as i32 =>
                {
                    let bank = status.bank_id.map(|bank_id| BankRef {
                        slot: status.slot,
                        bank_id,
                    });
                    if let Some(bank) = bank.filter(|bank| !self.complete.contains(bank)) {
                        self.cut_after(bank);
                    }
                }
                Some(UpdateOneof::Account(account))
                    if self.point == CutPoint::MidBank
                        && self.block_metas >= BLOCK_METAS_BEFORE_CUT =>
                {
                    let is_slot_history = account
                        .account
                        .as_ref()
                        .is_some_and(|info| info.pubkey == SLOT_HISTORY.as_ref());
                    let bank = account.bank_id.map(|bank_id| BankRef {
                        slot: account.slot,
                        bank_id,
                    });
                    if let Some(bank) =
                        bank.filter(|bank| is_slot_history && !self.complete.contains(bank))
                    {
                        self.cut_after(bank);
                    }
                }
                _ => {}
            }
        }
        Poll::Ready(item)
    }
}

/// A [`TonicGrpcConnector`] whose first connection is cut at a [`CutPoint`] by [`Cut`].
#[derive(Clone)]
struct CuttingConnector {
    inner: TonicGrpcConnector,
    point: CutPoint,
    log: Arc<Mutex<ConnectLog>>,
}

impl GrpcConnector for CuttingConnector {
    type Stream = Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send>>;
    type ConnectError = <TonicGrpcConnector as GrpcConnector>::ConnectError;
    type ConnectFuture =
        Pin<Box<dyn Future<Output = Result<Self::Stream, Self::ConnectError>> + Send>>;

    fn connect(
        &self,
        request: Arc<SubscribeRequest>,
        from_slot: Option<u64>,
    ) -> Self::ConnectFuture {
        let inner = self.inner.connect(request, from_slot);
        let log = Arc::clone(&self.log);
        let point = self.point;
        Box::pin(async move {
            let stream = inner.await?;
            let first = {
                let mut log = log.lock().unwrap();
                log.from_slots.push(from_slot);
                log.from_slots.len() == 1
            };
            log::info!("connected with from_slot={from_slot:?}");
            Ok(if first {
                Box::pin(Cut::new(stream, point, log)) as Self::Stream
            } else {
                Box::pin(stream) as Self::Stream
            })
        })
    }
}

/// Builds a [`CuttingConnector`] to the scenario's endpoint, sharing the stream's `request`.
fn cutting_connector(
    config: &RunConfig,
    point: CutPoint,
    request: &ReconnectRequest,
) -> Result<CuttingConnector> {
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
        .map(|token| token.parse())
        .transpose()
        .map_err(|error| anyhow::anyhow!("invalid x-token: {error}"))?;
    Ok(CuttingConnector {
        inner: TonicGrpcConnector::new(
            endpoint,
            ReconnectConfig::default(),
            x_token,
            Default::default(),
            // The stream's own request: a reconnect installs its sender there.
            request.clone(),
        ),
        point,
        log: Arc::default(),
    })
}

/// Sysvar writes, SlotHistory among them, and nothing else: small, and every bank has some.
fn accounts_request(commitment: CommitmentLevel) -> SubscribeRequest {
    SubscribeRequest {
        accounts: HashMap::from([(
            FILTER.to_owned(),
            SubscribeRequestFilterAccounts {
                owner: vec![SYSVAR_OWNER.to_owned()],
                ..Default::default()
            },
        )]),
        commitment: Some(commitment as i32),
        ..Default::default()
    }
}

/// Sysvar writes, block metas and commitment statuses.
fn request(commitment: CommitmentLevel) -> SubscribeRequest {
    SubscribeRequest {
        blocks_meta: HashMap::from([(
            FILTER.to_owned(),
            SubscribeRequestFilterBlocksMeta::default(),
        )]),
        slots: HashMap::from([(
            FILTER.to_owned(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        )]),
        ..accounts_request(commitment)
    }
}

/// The bank an update belongs to, for the updates this scenario subscribes to.
fn bank_of(update: &SubscribeUpdate) -> Option<BankRef> {
    match update.update_oneof.as_ref()? {
        UpdateOneof::Account(m) => m.bank_id.map(|bank_id| BankRef {
            slot: m.slot,
            bank_id,
        }),
        UpdateOneof::BlockMeta(m) => Some(BankRef {
            slot: m.slot,
            bank_id: m.bank_id,
        }),
        UpdateOneof::Slot(m) if is_created_bank(update) => m.bank_id.map(|bank_id| BankRef {
            slot: m.slot,
            bank_id,
        }),
        _ => None,
    }
}

/// Tells whether an update is a CreatedBank slot status.
const fn is_created_bank(update: &SubscribeUpdate) -> bool {
    matches!(update.update_oneof.as_ref(), Some(UpdateOneof::Slot(m))
        if m.status == SlotStatus::SlotCreatedBank as i32)
}

/// Tells whether an update is a bank's SlotHistory write, the last account write of every bank.
fn is_slot_history(update: &SubscribeUpdate) -> bool {
    matches!(update.update_oneof.as_ref(), Some(UpdateOneof::Account(m))
        if m.account.as_ref().is_some_and(|info| info.pubkey == SLOT_HISTORY.as_ref()))
}

/// Tells whether an update is the server's keep-alive ping or a pong, which match no filter.
const fn is_keepalive(update: &SubscribeUpdate) -> bool {
    matches!(
        update.update_oneof,
        Some(UpdateOneof::Ping(_) | UpdateOneof::Pong(_))
    )
}

/// The stream as a user sees it, split at the one [`Disconnected`] event.
#[derive(Debug, Default)]
struct Observed {
    /// Banks with an account write and no block meta when the stream disconnected.
    open_at_disconnect: HashSet<BankRef>,
    disconnected: Option<Disconnected>,
    /// Updates of the first connection, in order.
    before: Vec<SubscribeUpdate>,
    /// Updates of the second connection, in order.
    after: Vec<SubscribeUpdate>,
}

/// Block metas among updates, in order, as (slot, parent slot).
fn metas(updates: &[SubscribeUpdate]) -> impl Iterator<Item = (u64, u64)> + '_ {
    updates
        .iter()
        .filter_map(|update| match update.update_oneof.as_ref() {
            Some(UpdateOneof::BlockMeta(m)) => Some((m.slot, m.parent_slot)),
            _ => None,
        })
}

/// Reads the stream until the second connection delivered the SlotHistory writes of
/// [`BANKS_AFTER_RECONNECT`] banks past the cut bank, checking every update for a leaked
/// reconnect filter.
async fn observe(
    stream: &mut ReconnectStream<CuttingConnector>,
    log: &Mutex<ConnectLog>,
) -> Result<Observed> {
    let mut observed = Observed::default();
    let mut open = HashSet::new();
    let mut banks_past_cut = HashSet::new();
    tokio::time::timeout(TIMEOUT, async {
        while let Some(event) = stream.next().await {
            match event.context("reconnect stream failed")? {
                ReconnectEvent::Disconnected(disconnected) => {
                    ensure!(
                        observed.disconnected.is_none(),
                        "a second disconnect: {disconnected:?}"
                    );
                    log::info!("disconnected: {disconnected:?}");
                    observed.open_at_disconnect = std::mem::take(&mut open);
                    observed.disconnected = Some(disconnected);
                }
                ReconnectEvent::Update(update) if is_keepalive(&update) => {}
                ReconnectEvent::Update(update) => {
                    // Empty filters would be a hidden update let through.
                    ensure!(
                        !update.filters.is_empty()
                            && !update
                                .filters
                                .iter()
                                .any(|filter| filter == RECONNECT_FILTER_KEY),
                        "unexpected filters {:?} on {:?}",
                        update.filters,
                        update.update_oneof
                    );
                    if observed.disconnected.is_none() {
                        match (update.update_oneof.as_ref(), bank_of(&update)) {
                            (Some(UpdateOneof::BlockMeta(_)), Some(bank)) => {
                                open.remove(&bank);
                            }
                            (_, Some(bank)) => {
                                open.insert(bank);
                            }
                            _ => {}
                        }
                        observed.before.push(update);
                        continue;
                    }
                    let cut = log
                        .lock()
                        .unwrap()
                        .cut
                        .context("disconnected before the cut")?;
                    if is_slot_history(&update) {
                        if let Some(bank) = bank_of(&update).filter(|bank| bank.slot > cut.slot) {
                            banks_past_cut.insert(bank);
                        }
                    }
                    observed.after.push(update);
                    if banks_past_cut.len() >= BANKS_AFTER_RECONNECT {
                        return Ok(());
                    }
                }
            }
        }
        bail!("the reconnect stream ended")
    })
    .await
    .context("timed out waiting for the reconnect")??;
    Ok(observed)
}

/// Checks what every policy shares: one cut, reported in [`Disconnected::partial_banks`].
fn check_partial_banks(observed: &Observed, cut: BankRef) -> Result<&Disconnected> {
    let disconnected = observed
        .disconnected
        .as_ref()
        .context("no Disconnected event")?;
    ensure!(
        disconnected.partial_banks.contains(&cut),
        "the cut bank {cut:?} is not in {:?}",
        disconnected.partial_banks
    );
    for bank in &disconnected.partial_banks {
        ensure!(
            observed.open_at_disconnect.contains(bank),
            "{bank:?} is reported partial, but the user got its block meta or nothing of it"
        );
    }
    Ok(disconnected)
}

/// With replay, the stream reports the cut bank partial, replays from the last confirmed
/// slot, and resends the cut bank whole, with no slot missing across the reconnect.
#[test_helper(
    name = "reconnect-replay",
    tags = ["client", "reconnect"],
    params = [CommitmentLevel::Processed, CommitmentLevel::Confirmed, CommitmentLevel::Finalized]
)]
pub async fn reconnect_should_replay_partial_banks(
    config: &RunConfig,
    commitment: CommitmentLevel,
) -> Result<()> {
    let request = ReconnectRequest::new(request(commitment));
    let connector = cutting_connector(config, CutPoint::MidBank, &request)?;
    let log = Arc::clone(&connector.log);
    let mut stream = ReconnectStream::connect(
        connector,
        request,
        ReconnectConfig::default().with_policy(ReconnectPolicy::Replay { slots_behind: 0 }),
    )
    .await?;
    let observed = observe(&mut stream, &log).await?;
    let log = log.lock().unwrap();
    let cut = log.cut.context("no cut")?;

    let disconnected = check_partial_banks(&observed, cut)?;
    let from_slot = disconnected
        .replay_from_slot
        .context("no replay slot after three block metas")?;
    ensure!(
        from_slot <= cut.slot,
        "replay from {from_slot} skips the cut bank at {}",
        cut.slot
    );
    ensure!(
        log.from_slots == [None, Some(from_slot)],
        "connects {:?}, expected [None, Some({from_slot})]",
        log.from_slots
    );

    // The cut bank comes back whole: its SlotHistory write, then its block meta. Other
    // partial banks can be dead forks, which never get a block meta.
    let resent: Vec<_> = observed
        .after
        .iter()
        .filter(|update| bank_of(update) == Some(cut))
        .collect();
    let slot_history = resent.iter().position(|update| is_slot_history(update));
    let meta = resent
        .iter()
        .position(|update| matches!(update.update_oneof, Some(UpdateOneof::BlockMeta(_))));
    ensure!(
        matches!((slot_history, meta), (Some(write), Some(meta)) if write < meta),
        "the cut bank {cut:?} is not resent whole: SlotHistory at {slot_history:?}, block meta at {meta:?}"
    );

    // No slot is missing: each block meta past the cut has its parent's.
    let mut seen: HashSet<u64> = metas(&observed.before).map(|(slot, _)| slot).collect();
    let first = seen
        .iter()
        .min()
        .copied()
        .context("no block meta before the cut")?;
    for (slot, parent) in metas(&observed.after) {
        if slot > cut.slot {
            ensure!(
                parent < first || seen.contains(&parent),
                "slot {slot}'s parent {parent} never arrived"
            );
        }
        seen.insert(slot);
    }
    log::info!(
        "replay: cut {cut:?}, {} partial banks, replayed from {from_slot}",
        disconnected.partial_banks.len()
    );
    Ok(())
}

/// Without replay, the stream reports the cut bank partial, resumes at the tip, and never
/// delivers anything more of a bank it reported.
#[test_helper(
    name = "reconnect-no-replay",
    tags = ["client", "reconnect"],
    params = [CommitmentLevel::Processed, CommitmentLevel::Confirmed, CommitmentLevel::Finalized]
)]
pub async fn reconnect_should_resume_at_the_tip(
    config: &RunConfig,
    commitment: CommitmentLevel,
) -> Result<()> {
    let request = ReconnectRequest::new(request(commitment));
    let connector = cutting_connector(config, CutPoint::MidBank, &request)?;
    let log = Arc::clone(&connector.log);
    let mut stream = ReconnectStream::connect(
        connector,
        request,
        ReconnectConfig::default().with_policy(ReconnectPolicy::NoReplay),
    )
    .await?;
    let observed = observe(&mut stream, &log).await?;
    let log = log.lock().unwrap();
    let cut = log.cut.context("no cut")?;

    let disconnected = check_partial_banks(&observed, cut)?;
    ensure!(
        disconnected.replay_from_slot.is_none(),
        "no-replay reconnects from {:?}",
        disconnected.replay_from_slot
    );
    ensure!(
        log.from_slots == [None, None],
        "connects {:?}, expected [None, None]",
        log.from_slots
    );
    let partial: HashSet<_> = disconnected.partial_banks.iter().copied().collect();
    if let Some(leaked) = observed
        .after
        .iter()
        .find(|update| bank_of(update).is_some_and(|bank| partial.contains(&bank)))
    {
        bail!("a reported bank arrived again: {leaked:?}");
    }
    log::info!(
        "no-replay: cut {cut:?}, {} partial banks",
        disconnected.partial_banks.len()
    );
    Ok(())
}

/// Two filters, one for CreatedBank statuses and one for account data: a bank whose
/// CreatedBank arrived, but not all of its account data, is partial. The cut lands right after
/// a CreatedBank. The stream must report that bank; with replay it comes back with its
/// CreatedBank and all its account data, and without replay nothing more of it comes.
#[test_helper(
    name = "reconnect-created-bank-and-accounts",
    tags = ["client", "reconnect"],
    params = (
        [CommitmentLevel::Processed, CommitmentLevel::Confirmed, CommitmentLevel::Finalized],
        [ReconnectPolicy::NoReplay, ReconnectPolicy::Replay { slots_behind: 0 }],
    )
)]
pub async fn reconnect_should_discard_a_bank_with_created_bank_but_partial_accounts(
    config: &RunConfig,
    commitment: CommitmentLevel,
    policy: ReconnectPolicy,
) -> Result<()> {
    const CREATED: &str = "created";
    const ACCOUNTS: &str = "accounts";
    let request = SubscribeRequest {
        slots: HashMap::from([(
            CREATED.to_owned(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            },
        )]),
        accounts: HashMap::from([(
            ACCOUNTS.to_owned(),
            SubscribeRequestFilterAccounts {
                owner: vec![SYSVAR_OWNER.to_owned()],
                ..Default::default()
            },
        )]),
        commitment: Some(commitment as i32),
        ..Default::default()
    };
    let request = ReconnectRequest::new(request);
    let connector = cutting_connector(config, CutPoint::AfterCreatedBank, &request)?;
    let log = Arc::clone(&connector.log);
    let mut stream = ReconnectStream::connect(
        connector,
        request,
        ReconnectConfig::default().with_policy(policy),
    )
    .await?;
    let observed = observe(&mut stream, &log).await?;
    let log = log.lock().unwrap();
    let cut = log.cut.context("no cut")?;

    // What the user held of the cut bank when the connection dropped.
    let of_cut = |update: &&SubscribeUpdate| bank_of(update) == Some(cut);
    let held: Vec<_> = observed.before.iter().filter(of_cut).collect();
    let created = held
        .iter()
        .find(|update| is_created_bank(update))
        .context("the cut bank's CreatedBank was not delivered before the cut")?;
    ensure!(
        created.filters == [CREATED],
        "CreatedBank came with filters {:?}",
        created.filters
    );
    let accounts = held
        .iter()
        .filter(|update| matches!(update.update_oneof, Some(UpdateOneof::Account(_))))
        .count();
    let slot_history = held.iter().any(|update| is_slot_history(update));
    if commitment == CommitmentLevel::Processed {
        // Agave writes Clock and SlotHashes before CreatedBank, SlotHistory at freeze.
        ensure!(
            accounts > 0 && !slot_history,
            "expected some but not all account data of {cut:?} before the cut: {accounts} \
             writes, SlotHistory {slot_history}"
        );
    }

    let disconnected = check_partial_banks(&observed, cut)?;
    let from_slot = disconnected.replay_from_slot;
    ensure!(
        log.from_slots == [None, from_slot],
        "connects {:?}, expected [None, {from_slot:?}]",
        log.from_slots
    );
    let resent: Vec<_> = observed.after.iter().filter(of_cut).collect();
    match policy {
        ReconnectPolicy::NoReplay => {
            ensure!(
                from_slot.is_none(),
                "no-replay reconnects from {from_slot:?}"
            );
            if let Some(leaked) = resent.first() {
                bail!("the discarded bank {cut:?} arrived again: {leaked:?}");
            }
        }
        ReconnectPolicy::Replay { .. } => {
            let from_slot = from_slot.context("no replay slot after three block metas")?;
            ensure!(
                from_slot <= cut.slot,
                "replay from {from_slot} skips the cut bank at {}",
                cut.slot
            );
            ensure!(
                resent.iter().any(|update| is_created_bank(update))
                    && resent.iter().any(|update| is_slot_history(update)),
                "the replay did not resend {cut:?} whole: {} updates of it",
                resent.len()
            );
        }
    }
    log::info!(
        "created-bank-and-accounts: cut after {cut:?}'s CreatedBank, holding {accounts} account \
         writes (SlotHistory {slot_history}), reported {:?}, replayed from {from_slot:?}, {} \
         updates of it resent",
        disconnected.partial_banks,
        resent.len()
    );
    Ok(())
}

/// A subscription to sysvar accounts alone, with no block meta or slot filter of its own,
/// relies on the hidden reconnect filters: the stream still reports the cut bank partial and
/// takes the replay slot from confirmed slots the user never sees.
#[test_helper(
    name = "reconnect-accounts-only",
    tags = ["client", "reconnect"],
    params = (
        [CommitmentLevel::Processed, CommitmentLevel::Confirmed, CommitmentLevel::Finalized],
        [ReconnectPolicy::NoReplay, ReconnectPolicy::Replay { slots_behind: 0 }],
    )
)]
pub async fn reconnect_should_serve_accounts_only_subscriptions(
    config: &RunConfig,
    commitment: CommitmentLevel,
    policy: ReconnectPolicy,
) -> Result<()> {
    let request = ReconnectRequest::new(accounts_request(commitment));
    let connector = cutting_connector(config, CutPoint::MidBank, &request)?;
    let log = Arc::clone(&connector.log);
    let mut stream = ReconnectStream::connect(
        connector,
        request,
        ReconnectConfig::default().with_policy(policy),
    )
    .await?;
    let observed = observe(&mut stream, &log).await?;
    let log = log.lock().unwrap();
    let cut = log.cut.context("no cut")?;

    if let Some(leaked) = observed
        .before
        .iter()
        .chain(&observed.after)
        .find(|update| !matches!(update.update_oneof, Some(UpdateOneof::Account(_))))
    {
        bail!("only accounts were asked for, got {leaked:?}");
    }
    let disconnected = check_partial_banks(&observed, cut)?;
    let from_slot = disconnected.replay_from_slot;
    ensure!(
        log.from_slots == [None, from_slot],
        "connects {:?}, expected [None, {from_slot:?}]",
        log.from_slots
    );
    match policy {
        ReconnectPolicy::NoReplay => {
            ensure!(
                from_slot.is_none(),
                "no-replay reconnects from {from_slot:?}"
            );
            let partial: HashSet<_> = disconnected.partial_banks.iter().copied().collect();
            if let Some(leaked) = observed
                .after
                .iter()
                .find(|update| bank_of(update).is_some_and(|bank| partial.contains(&bank)))
            {
                bail!("a reported bank arrived again: {leaked:?}");
            }
        }
        ReconnectPolicy::Replay { .. } => {
            let from_slot =
                from_slot.context("no replay slot: the hidden slot statuses never arrived")?;
            ensure!(
                from_slot <= cut.slot,
                "replay from {from_slot} skips the cut bank at {}",
                cut.slot
            );
            ensure!(
                observed
                    .after
                    .iter()
                    .any(|update| bank_of(update) == Some(cut) && is_slot_history(update)),
                "the replay did not resend the cut bank {cut:?}'s SlotHistory write"
            );
        }
    }
    log::info!(
        "accounts-only: cut {cut:?}, {} partial banks, replayed from {from_slot:?}",
        disconnected.partial_banks.len()
    );
    Ok(())
}

/// A subscription to block metas alone holds no bank in part: a disconnect reports no partial
/// bank. With replay, the block metas from the replay slot come again and no slot is missing;
/// without replay, no block meta comes twice.
#[test_helper(
    name = "reconnect-block-meta-only",
    tags = ["client", "reconnect"],
    params = (
        [CommitmentLevel::Processed, CommitmentLevel::Confirmed, CommitmentLevel::Finalized],
        [ReconnectPolicy::NoReplay, ReconnectPolicy::Replay { slots_behind: 0 }],
    )
)]
pub async fn reconnect_should_serve_block_meta_only_subscriptions(
    config: &RunConfig,
    commitment: CommitmentLevel,
    policy: ReconnectPolicy,
) -> Result<()> {
    sealing_only(config, commitment, policy, Sealing::BlockMeta).await
}

/// A Block holds its whole bank. Cut right after one, before its block meta: the stream must
/// not report the bank partial, and without replay must not drop the rest of it.
#[test_helper(
    name = "reconnect-blocks-only",
    tags = ["client", "reconnect"],
    params = (
        [CommitmentLevel::Processed, CommitmentLevel::Confirmed, CommitmentLevel::Finalized],
        [ReconnectPolicy::NoReplay, ReconnectPolicy::Replay { slots_behind: 0 }],
    )
)]
pub async fn reconnect_should_serve_blocks_only_subscriptions(
    config: &RunConfig,
    commitment: CommitmentLevel,
    policy: ReconnectPolicy,
) -> Result<()> {
    sealing_only(config, commitment, policy, Sealing::Block).await
}

/// A block footer follows all of its bank's content. Cut right after one, before its block
/// meta: the stream must not report the bank partial. Alpenglow clusters only.
#[test_helper(
    name = "reconnect-block-footers-only",
    tags = ["client", "reconnect", "block-footer"],
    params = (
        [CommitmentLevel::Processed, CommitmentLevel::Confirmed, CommitmentLevel::Finalized],
        [ReconnectPolicy::NoReplay, ReconnectPolicy::Replay { slots_behind: 0 }],
    )
)]
pub async fn reconnect_should_serve_block_footers_only_subscriptions(
    config: &RunConfig,
    commitment: CommitmentLevel,
    policy: ReconnectPolicy,
) -> Result<()> {
    sealing_only(config, commitment, policy, Sealing::BlockFooter).await
}

/// Subscribes to one [`Sealing`] update alone, cuts after a few, and checks the reconnect:
/// - no bank is reported partial, since each update is the whole of what the user gets of it;
/// - with replay, every update from the replay slot comes again, and none is missing;
/// - without replay, none comes twice.
async fn sealing_only(
    config: &RunConfig,
    commitment: CommitmentLevel,
    policy: ReconnectPolicy,
    sealing: Sealing,
) -> Result<()> {
    let request = ReconnectRequest::new(sealing.request(commitment));
    let connector = cutting_connector(config, CutPoint::After(sealing), &request)?;
    let log = Arc::clone(&connector.log);
    let mut stream = ReconnectStream::connect(
        connector,
        request,
        ReconnectConfig::default().with_policy(policy),
    )
    .await?;

    // Updates as (bank, parent slot), before and after the disconnect.
    let mut before = Vec::new();
    let mut after = Vec::new();
    let mut disconnected = None;
    tokio::time::timeout(TIMEOUT, async {
        while let Some(event) = stream.next().await {
            let update = match event.context("reconnect stream failed")? {
                ReconnectEvent::Disconnected(event) => {
                    ensure!(disconnected.is_none(), "a second disconnect: {event:?}");
                    log::info!("disconnected: {event:?}");
                    disconnected = Some(event);
                    continue;
                }
                ReconnectEvent::Update(update) if is_keepalive(&update) => continue,
                ReconnectEvent::Update(update) => update,
            };
            ensure!(update.filters == [FILTER], "leaked {update:?}");
            let entry = sealing
                .bank_of(&update)
                .with_context(|| format!("only {sealing:?} was asked for, got {update:?}"))?;
            if disconnected.is_none() {
                before.push(entry);
                continue;
            }
            after.push(entry);
            let cut = log
                .lock()
                .unwrap()
                .cut
                .context("disconnected before the cut")?;
            if after
                .iter()
                .filter(|(bank, _)| bank.slot > cut.slot)
                .count()
                >= BANKS_AFTER_RECONNECT
            {
                return Ok(());
            }
        }
        bail!("the reconnect stream ended")
    })
    .await
    .context("timed out waiting for the reconnect")??;

    let log = log.lock().unwrap();
    let cut = log.cut.context("no cut")?;
    let disconnected = disconnected.context("no Disconnected event")?;
    ensure!(
        disconnected.partial_banks.is_empty(),
        "a {sealing:?}-only subscription has no partial bank, got {:?}",
        disconnected.partial_banks
    );
    let from_slot = disconnected.replay_from_slot;
    ensure!(
        log.from_slots == [None, from_slot],
        "connects {:?}, expected [None, {from_slot:?}]",
        log.from_slots
    );
    ensure!(
        before.iter().any(|(bank, _)| *bank == cut),
        "the cut bank {cut:?} was not delivered before the cut"
    );

    match policy {
        ReconnectPolicy::NoReplay => {
            ensure!(
                from_slot.is_none(),
                "no-replay reconnects from {from_slot:?}"
            );
            let mut seen = HashSet::new();
            for (bank, _) in before.iter().chain(&after) {
                ensure!(seen.insert(*bank), "{sealing:?} of {bank:?} came twice");
            }
        }
        ReconnectPolicy::Replay { .. } => {
            let from_slot = from_slot.context("no replay slot after the first updates")?;
            ensure!(
                from_slot <= cut.slot,
                "replay from {from_slot} skips the last {sealing:?}, at {}",
                cut.slot
            );
            let resent: HashSet<_> = after.iter().map(|(bank, _)| *bank).collect();
            for (bank, _) in &before {
                ensure!(
                    bank.slot < from_slot || resent.contains(bank),
                    "the replay from {from_slot} did not resend the {sealing:?} of {bank:?}"
                );
            }
            // A footer has no parent slot: only Blocks and block metas show a missing slot.
            let mut seen: HashSet<u64> = before.iter().map(|(bank, _)| bank.slot).collect();
            let first = before
                .iter()
                .map(|(bank, _)| bank.slot)
                .min()
                .context("nothing before the cut")?;
            for (bank, parent) in &after {
                if let Some(parent) = parent {
                    ensure!(
                        *parent < first || seen.contains(parent),
                        "slot {}'s parent {parent} never arrived",
                        bank.slot
                    );
                }
                seen.insert(bank.slot);
            }
        }
    }
    log::info!(
        "{sealing:?}-only: cut after {cut:?}, replayed from {from_slot:?}, {} before, {} after",
        before.len(),
        after.len()
    );
    Ok(())
}

/// `subscribe_with_reconnect` hides its own filters, and a filter sent on its sink applies to
/// the live connection.
#[test_helper(name = "reconnect-sink-filters", tags = ["client", "reconnect"])]
pub async fn reconnect_sink_should_update_filters(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;
    let first = SubscribeRequest {
        blocks_meta: HashMap::from([(
            FILTER.to_owned(),
            SubscribeRequestFilterBlocksMeta::default(),
        )]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };
    let (mut sink, mut stream) = client.subscribe_with_reconnect(Some(first.clone())).await?;

    tokio::time::timeout(TIMEOUT, async {
        // Before the update: block metas only. A slot status here is the reconnect filter's.
        let mut metas = 0;
        while metas < BLOCK_METAS_BEFORE_CUT {
            let event = stream.next().await.context("stream ended")??;
            let ReconnectEvent::Update(update) = event else {
                bail!("unexpected {event:?}");
            };
            if is_keepalive(&update) {
                continue;
            }
            ensure!(update.filters == [FILTER], "leaked {update:?}");
            ensure!(
                matches!(update.update_oneof, Some(UpdateOneof::BlockMeta(_))),
                "only block metas were asked for, got {update:?}"
            );
            metas += 1;
        }

        let mut second = first;
        second.slots.insert(
            "late".to_owned(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        );
        sink.send(second).await?;

        loop {
            let event = stream.next().await.context("stream ended")??;
            let ReconnectEvent::Update(update) = event else {
                bail!("unexpected {event:?}");
            };
            ensure!(
                !update
                    .filters
                    .iter()
                    .any(|filter| filter == RECONNECT_FILTER_KEY),
                "leaked {update:?}"
            );
            if matches!(update.update_oneof, Some(UpdateOneof::Slot(_))) {
                ensure!(
                    update.filters == ["late"],
                    "slot status with {:?}",
                    update.filters
                );
                return Ok(());
            }
        }
    })
    .await
    .context("timed out waiting for the late slot filter")?
}
