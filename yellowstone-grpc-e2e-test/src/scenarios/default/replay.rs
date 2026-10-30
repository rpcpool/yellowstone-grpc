use {
    crate::{grpc::new_client, scenarios::RunConfig},
    anyhow::{bail, ensure, Context, Result},
    futures::SinkExt,
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        str::FromStr,
        time::Duration,
    },
    tokio_stream::StreamExt,
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto, SlotStatus,
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    },
};

/// Sysvar accounts every bank writes. Agave writes Clock and SlotHashes while it constructs the
/// bank, before CreatedBank, RecentBlockhashes at the block's last tick, and SlotHistory when
/// the block freezes.
const MUST_HAVE_SYSVAR_ACCOUNTS: [&str; 4] = [
    // emitted before CreatedBank
    "SysvarC1ock11111111111111111111111111111111",
    "SysvarS1otHashes111111111111111111111111111",
    // written when the block freeze
    "SysvarS1otHistory11111111111111111111111111",
    "SysvarRecentB1ockHashes11111111111111111111",
];

/// Fixed-address accounts the first bank of a new epoch on its fork writes while agave
/// constructs it, before CreatedBank. Mirrors `SYSVAR_ON_EPOCH_CROSSING` in the geyser plugin's
/// block reconstruction.
const EPOCH_CROSSING_ACCOUNTS: [&str; 5] = [
    // every epoch crossing
    "SysvarStakeHistory1111111111111111111111111",
    "SysvarEpochRewards1111111111111111111111111",
    // Alpenglow (or migration epoch): reward-epoch delegated stakes account
    "CHfmHwNcskfxg1ZnLatbHbmVvYbA6rLDz3Xok5a3Ku8i",
    // Alpenglow: the incinerator receives the VAT burned from staked vote accounts
    "1nc1nerator11111111111111111111111111111111",
    // Alpenglow: vote reward account
    "3vJzniWALu2qGFJ1ZY6JD3Y2ZmA5zWqmVdDX7yBXHgu7",
];

fn must_have_sysvar_pubkeys() -> Result<HashSet<Pubkey>> {
    MUST_HAVE_SYSVAR_ACCOUNTS
        .iter()
        .map(|s| Pubkey::from_str(s).context("valid must-have sysvar pubkey string"))
        .collect()
}

/// The must-have sysvar accounts and the epoch-crossing accounts: the fixed-address accounts
/// that may arrive before a bank's CreatedBank.
fn before_created_bank_pubkeys() -> Result<HashSet<Pubkey>> {
    MUST_HAVE_SYSVAR_ACCOUNTS
        .iter()
        .chain(EPOCH_CROSSING_ACCOUNTS.iter())
        .map(|s| Pubkey::from_str(s).context("valid pubkey string"))
        .collect()
}

/// How many slots `it_should_support_replay` checks once they reach its commitment level.
const REPLAY_SLOTS_TO_CHECK: usize = 10;

/// Verifies replay support by receiving historical data from a replay request.
/// Replays from a depth below the processed tip that depends on `commitment`, and checks the
/// first [`REPLAY_SLOTS_TO_CHECK`] replayed slots that reach `commitment`.
/// Each replayed bank's CreatedBank comes before its block meta, and before its accounts other
/// than the must-have sysvars and epoch-crossing accounts written while the bank is
/// constructed, as in the live stream.
/// Each replayed bank brings every must-have sysvar account.
/// Each checked slot gets every slot status up to `commitment`, in order: Processed, then
/// Confirmed, then Finalized.
/// At processed, each bank must also bring all its transactions before its block meta.
#[test_helper(
    name = "replay",
    tags = ["replay"],
    params = [
        CommitmentLevelProto::Processed,
        CommitmentLevelProto::Confirmed,
        CommitmentLevelProto::Finalized,
    ]
)]
pub async fn it_should_support_replay(
    config: &RunConfig,
    commitment: CommitmentLevelProto,
) -> Result<()> {
    // Confirmed and finalized lag the processed tip, so replay starts deep enough to find
    // REPLAY_SLOTS_TO_CHECK slots that already reached the level.
    // A slot reaches `commitment` with the last of `expected_statuses`, after the others.
    let (replay_depth, expected_statuses): (u64, &[SlotStatus]) = match commitment {
        CommitmentLevelProto::Processed => (10, &[SlotStatus::SlotProcessed]),
        CommitmentLevelProto::Confirmed => {
            (100, &[SlotStatus::SlotProcessed, SlotStatus::SlotConfirmed])
        }
        CommitmentLevelProto::Finalized => (
            350,
            &[
                SlotStatus::SlotProcessed,
                SlotStatus::SlotConfirmed,
                SlotStatus::SlotFinalized,
            ],
        ),
    };
    let reached_status = *expected_statuses
        .last()
        .context("every commitment expects a slot status")?;
    let mut client = new_client(config).await?;

    let resp = client.get_slot(None).await.context("get_slot")?;
    let tip = resp.slot;
    let musthave_pubkeys = must_have_sysvar_pubkeys()?;
    let before_created_bank = before_created_bank_pubkeys()?;
    let account_filter = SubscribeRequestFilterAccounts {
        account: MUST_HAVE_SYSVAR_ACCOUNTS.map(str::to_string).to_vec(),
        ..Default::default()
    };
    let from_slot = tip.saturating_sub(replay_depth);
    let subscription = SubscribeRequest {
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]),
        accounts: HashMap::from([("test".to_string(), account_filter)]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        commitment: Some(commitment as i32),
        from_slot: Some(from_slot),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;
    let mut count = 0usize;
    const MAX_UPDATES: usize = 10;
    log::info!("current tip slot is {tip}, subscribing at {commitment:?} from slot {from_slot}");
    // Slots whose status reached `commitment`.
    let mut reached_slots = HashSet::new();
    let mut block_visited = HashSet::new();
    let mut block_meta_received = HashSet::new();
    let mut slot_status_received: HashMap<u64, HashSet<SlotStatus>> = HashMap::new();
    // Keyed by (slot, bank_id): a slot can have more than one bank across forks.
    let mut created_banks = HashSet::new();
    let mut musthave_seen: HashMap<(u64, u64), HashSet<Pubkey>> = HashMap::new();
    while let Some(update) = stream.next().await {
        if reached_slots.len() >= REPLAY_SLOTS_TO_CHECK {
            break;
        }
        let update = update.context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Slot(slot) => {
                ensure!(
                    slot.parent.is_some(),
                    "slot update should have parent slot for replayed slots"
                );
                // CreatedBank opens a bank, so it comes first, not last.
                if slot.status() == SlotStatus::SlotCreatedBank {
                    let bank_id = slot.bank_id.with_context(|| {
                        format!("slot {}: CreatedBank should have a bank_id", slot.slot)
                    })?;
                    created_banks.insert((slot.slot, bank_id));
                    continue;
                }
                let statuses = slot_status_received.entry(slot.slot).or_default();
                statuses.insert(slot.status());
                if block_visited.contains(&slot.slot) {
                    count += 1;
                }
                if slot.status() == reached_status {
                    ensure!(
                        slot.slot >= from_slot,
                        "slot {}: replay sent a slot before from_slot {from_slot}",
                        slot.slot
                    );
                    let missing: Vec<&SlotStatus> = expected_statuses
                        .iter()
                        .filter(|status| !statuses.contains(status))
                        .collect();
                    ensure!(
                        missing.is_empty(),
                        "slot {}: reached {commitment:?} without first receiving slot statuses {missing:?}",
                        slot.slot
                    );
                    reached_slots.insert(slot.slot);
                }
            }
            UpdateOneof::Account(subscribe_update_account) => {
                let account = subscribe_update_account
                    .account
                    .context("account update should have account field")?;
                let actual_pubkey = Pubkey::try_from(account.pubkey.clone())
                    .map_err(|_| anyhow::anyhow!("invalid account pubkey bytes"))?;
                ensure!(
                    musthave_pubkeys.contains(&actual_pubkey),
                    "received unexpected pubkey {actual_pubkey}"
                );
                block_visited.insert(subscribe_update_account.slot);

                let bank_id = subscribe_update_account
                    .bank_id
                    .context("replayed account update should have a bank_id")?;
                musthave_seen
                    .entry((subscribe_update_account.slot, bank_id))
                    .or_default()
                    .insert(actual_pubkey);
                // Agave writes some must-have sysvars, and the epoch-crossing accounts on the
                // first bank of an epoch, while it constructs the bank, before CreatedBank; any
                // other account comes after it.
                ensure!(
                    subscribe_update_account.slot >= tip
                        || created_banks.contains(&(subscribe_update_account.slot, bank_id))
                        || before_created_bank.contains(&actual_pubkey),
                    "slot {} bank_id {bank_id}: account {actual_pubkey} arrived before CreatedBank, but only must-have sysvar and epoch-crossing accounts may",
                    subscribe_update_account.slot
                );

                ensure!(
                    !block_meta_received.contains(&subscribe_update_account.slot),
                    "block meta should always be last to arrive for a slot, after all account updates have been received"
                );

                ensure!(
                    !slot_status_received.contains_key(&subscribe_update_account.slot),
                    "slot status should always be last to arrive for a slot, after all account updates have been received"
                );

                log::info!(
                    "received account update for slot {} {count}/{MAX_UPDATES}",
                    subscribe_update_account.slot
                );
            }
            UpdateOneof::BlockMeta(ev) => {
                block_meta_received.insert(ev.slot);
                ensure!(
                    ev.slot >= tip || created_banks.contains(&(ev.slot, ev.bank_id)),
                    "slot {} bank_id {}: CreatedBank should arrive before the bank's block meta",
                    ev.slot,
                    ev.bank_id
                );
                ensure!(
                    !slot_status_received.contains_key(&ev.slot),
                    "slot status should always be last to arrive for a slot, after all block meta updates have been received"
                );
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => continue,
            other => bail!("unexpected update type: {other:?}"),
        }
    }
    ensure!(
        reached_slots.len() >= REPLAY_SLOTS_TO_CHECK,
        "stream ended after {} of {REPLAY_SLOTS_TO_CHECK} slots reached {commitment:?}",
        reached_slots.len()
    );
    let created_bank_slots: HashSet<u64> = created_banks.iter().map(|(slot, _)| *slot).collect();
    let mut slots_without_created_bank: Vec<u64> = reached_slots
        .iter()
        .filter(|slot| !created_bank_slots.contains(slot))
        .copied()
        .collect();
    slots_without_created_bank.sort_unstable();
    ensure!(
        slots_without_created_bank.is_empty(),
        "should receive CreatedBank for every slot that reached {commitment:?}, missing for slots {slots_without_created_bank:?}"
    );
    let mut banks_missing_musthave: Vec<(u64, u64, Vec<Pubkey>)> = created_banks
        .iter()
        .filter(|(slot, _)| reached_slots.contains(slot))
        .filter_map(|&(slot, bank_id)| {
            let seen = musthave_seen.get(&(slot, bank_id));
            let missing: Vec<Pubkey> = musthave_pubkeys
                .iter()
                .filter(|pubkey| !seen.is_some_and(|seen| seen.contains(pubkey)))
                .copied()
                .collect();
            (!missing.is_empty()).then_some((slot, bank_id, missing))
        })
        .collect();
    banks_missing_musthave.sort_unstable();
    ensure!(
        banks_missing_musthave.is_empty(),
        "every replayed bank should bring all must-have sysvar accounts, missing (slot, bank_id, accounts): {banks_missing_musthave:?}"
    );

    if commitment != CommitmentLevelProto::Processed {
        return Ok(());
    }
    // Several connects: each one lands in a different part of the bank in progress.
    for round in 0..PROCESSED_REPLAY_ROUNDS {
        tokio::time::timeout(
            PROCESSED_REPLAY_TIMEOUT,
            replay_delivers_whole_banks_at_processed(config, round),
        )
        .await
        .context("timed out waiting for block metas")??;
    }

    Ok(())
}

const PROCESSED_REPLAY_ROUNDS: usize = 5;
const PROCESSED_REPLAY_SLOTS_BEHIND: u64 = 3;
const PROCESSED_REPLAY_SLOTS_AFTER: u64 = 3;
const PROCESSED_REPLAY_TIMEOUT: Duration = Duration::from_secs(60);

/// Connects at processed a few slots behind the tip and reads a few slots past it.
/// Every bank must bring `executed_transaction_count` transactions before its block meta.
async fn replay_delivers_whole_banks_at_processed(config: &RunConfig, round: usize) -> Result<()> {
    let mut client = new_client(config).await?;
    let tip = client
        .get_slot(Some(CommitmentLevelProto::Processed))
        .await
        .context("get_slot")?
        .slot;
    let from_slot = tip.saturating_sub(PROCESSED_REPLAY_SLOTS_BEHIND);
    let subscription = SubscribeRequest {
        transactions: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        commitment: Some(CommitmentLevelProto::Processed as i32),
        from_slot: Some(from_slot),
        ..Default::default()
    };
    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;
    log::info!("round {round}: tip {tip}, subscribing from slot {from_slot}");

    // Keyed by (slot, bank_id): one slot can have more than one bank.
    let mut signatures: HashMap<(u64, u64), HashSet<Vec<u8>>> = HashMap::new();
    let mut checked = HashSet::new();
    let mut short = Vec::new();

    while let Some(update) = stream.next().await {
        let update = update.context("stream should yield updates without error")?;
        match update.update_oneof {
            Some(UpdateOneof::Transaction(tx)) => {
                let key = (tx.slot, tx.bank_id);
                if let Some(info) = tx.transaction {
                    signatures.entry(key).or_default().insert(info.signature);
                }
            }
            Some(UpdateOneof::BlockMeta(meta)) => {
                let key = (meta.slot, meta.bank_id);
                // A bank that seals while the client connects comes in the replay and live.
                if meta.slot < from_slot || !checked.insert(key) {
                    continue;
                }
                let received = signatures.remove(&key).map_or(0, |set| set.len() as u64);
                log::info!(
                    "round {round}: slot {} bank {}: {received}/{} transactions",
                    meta.slot,
                    meta.bank_id,
                    meta.executed_transaction_count,
                );
                if received != meta.executed_transaction_count {
                    short.push((meta.slot, received, meta.executed_transaction_count));
                }
                if meta.slot >= tip + PROCESSED_REPLAY_SLOTS_AFTER {
                    break;
                }
            }
            _ => {}
        }
    }

    ensure!(
        !checked.is_empty(),
        "round {round}: no block meta received from slot {from_slot}"
    );
    ensure!(
        short.is_empty(),
        "round {round}: banks with missing transactions as (slot, received, executed): {short:?}"
    );
    Ok(())
}

/// Verifies replay support by receiving historical data from a replay request.
#[test_helper(name = "block-subscription-replay", tags = ["replay"])]
pub async fn it_should_support_block_subscription_replay(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;

    let resp = client.get_slot(None).await.context("get_slot")?;
    let tip = resp.slot;
    let sysvar_clock_str = "SysvarC1ock11111111111111111111111111111111";
    let from_slot = tip.saturating_sub(10);
    let subscription = SubscribeRequest {
        blocks: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterBlocks {
                account_include: vec![sysvar_clock_str.to_string()],
                include_accounts: Some(true),
                include_transactions: Some(true),
                include_entries: Some(true),
                ..Default::default()
            },
        )]),
        from_slot: Some(from_slot),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;
    log::info!(
        "current tip slot is {}, subscribing from slot {}",
        tip,
        from_slot
    );
    let mut remaining_slot_to_visit = Vec::from_iter(from_slot..tip);
    let mut visited = HashSet::new();
    while let Some(update) = stream.next().await {
        if remaining_slot_to_visit.is_empty() {
            break;
        }
        let update = update.context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        if let UpdateOneof::Block(slot) = update_oneof {
            log::info!("received block update for slot {}", slot.slot);
            remaining_slot_to_visit.retain(|s| *s != slot.slot);
            if !visited.insert(slot.slot) {
                bail!("received duplicate block update for slot {}", slot.slot);
            }
        }
    }
    ensure!(
        remaining_slot_to_visit.is_empty(),
        "should have received updates for all expected slots in the replay"
    );

    Ok(())
}

/// How many slots behind the processed tip [`it_should_replay_after_sdk_subscribe`] replays
/// from.
const SDK_SUBSCRIBE_REPLAY_DEPTH: u64 = 10;
/// How many slots past `from_slot` the first replayed block meta may be, for slots the leader
/// skipped.
const SDK_SUBSCRIBE_SKIPPED_SLOTS: u64 = 3;

/// Verifies a replay asked for the way users ask with the SDK: `subscribe()`, which sends an
/// empty request first, then the request with `from_slot` on the sink.
///
/// ```text
/// client:  subscribe() ──► {}                      asks for nothing: not the first request
///          sink.send   ──► { blocks_meta, from_slot }  the first request: replays
/// ```
///
/// The server must not take the empty request as the first request, which alone can replay.
/// The check: a block meta from near `from_slot` arrives, which only the replay can send, since
/// those slots froze before the client connected.
#[test_helper(name = "replay-after-sdk-subscribe", tags = ["replay"])]
pub async fn it_should_replay_after_sdk_subscribe(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;
    let tip = client.get_slot(None).await.context("get_slot")?.slot;
    let from_slot = tip.saturating_sub(SDK_SUBSCRIBE_REPLAY_DEPTH);

    let (mut sink, mut stream) = client.subscribe().await.context("subscribe")?;
    sink.send(SubscribeRequest {
        blocks_meta: HashMap::from([("meta".to_string(), Default::default())]),
        from_slot: Some(from_slot),
        ..Default::default()
    })
    .await
    .context("send the request with from_slot")?;

    let first_block_meta = tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(update) = stream.next().await {
            let update = update.context("stream should yield updates without error")?;
            if let Some(UpdateOneof::BlockMeta(block_meta)) = update.update_oneof {
                return Ok(block_meta.slot);
            }
        }
        bail!("stream ended before any block meta")
    })
    .await
    .context("no block meta within 30s")??;

    log::info!("tip {tip}, from_slot {from_slot}, first block meta for slot {first_block_meta}");
    ensure!(
        first_block_meta <= from_slot + SDK_SUBSCRIBE_SKIPPED_SLOTS,
        "the first block meta is for slot {first_block_meta}, not near from_slot {from_slot}: \
         the replay was ignored (tip at connect {tip})"
    );
    Ok(())
}

/// How many slots before the commitment's own tip [`it_should_join_replay_and_live_stream`]
/// replays from. Starting from the confirmed or finalized tip, not the processed one, reaches
/// further back at confirmed and further still at finalized, so the replay holds banks already
/// at that commitment, while staying inside the server's replay buffer.
const JOIN_REPLAY_DEPTH: u64 = 10;

/// Banks that reach the commitment, past the tip at connect, before the test stops.
const JOIN_LIVE_BANKS_TO_CHECK: usize = 3;
/// How many slots past the processed tip at connect
/// [`it_should_replay_contiguous_slots_from_the_tip`] reads.
const CONTIGUOUS_SLOTS_PAST_TIP: u64 = 10;

/// When [`join_replay_and_live_stream`] stops reading.
#[derive(Clone, Copy)]
enum JoinUntil {
    /// Once this many banks at or past the processed tip at connect reached the commitment.
    LiveBanks(usize),
    /// Once the slot this far past the processed tip at connect reached the commitment. Every
    /// slot from `from_slot` to it must have reached it too, so a slot its leader skipped fails
    /// the check. See [`it_should_replay_contiguous_slots_from_the_tip`].
    ContiguousPastTip(u64),
}
const JOIN_TIMEOUT: Duration = Duration::from_secs(180);

/// What a client received of one bank, keyed by `(slot, bank_id)`.
#[derive(Default)]
struct JoinedBank {
    created_bank: bool,
    transactions: HashSet<Vec<u8>>,
    entries: HashSet<u64>,
    block: bool,
    footer: bool,
    block_meta: bool,
    /// Commitment statuses, in arrival order.
    statuses: Vec<SlotStatus>,
}

/// Verifies a `from_slot` replay joins the live stream with every bank once, whole and in
/// order, at `commitment`, across the switch from replay to live:
///
/// ```text
/// time ───────────────────────────────────────────────────────────────────────►
/// channel:   from_slot ... commitment tip ... processed tip │ ...
///                                                         L, R
/// replay:    from_slot ... commitment tip                     (already at `commitment`)
/// live:                                     ... processed tip   ...  (reach it later)
/// client:    every bank once: content, Block, footer, block meta, then Processed,
///            Confirmed, Finalized in order
///
/// from_slot = commitment tip - JOIN_REPLAY_DEPTH
/// ```
///
/// For every bank from `from_slot` on:
/// - its CreatedBank arrives once, at every commitment, before its block meta;
/// - no transaction or entry arrives twice, and all of them arrive before its Block;
/// - then the Block, then the footer (Alpenglow), then the block meta, each once;
/// - every transaction and entry the block meta counts has arrived by then;
/// - its commitment statuses arrive after its block meta, each once, in the order
///   Processed, Confirmed, Finalized;
///
/// It stops once `JOIN_LIVE_BANKS_TO_CHECK` banks at or past the processed tip at connect
/// reached `commitment`, and fails if no bank at or before the commitment's tip at connect did,
/// so both replayed and live banks are checked.
#[test_helper(
    name = "replay-ordering",
    tags = ["ordering", "replay"],
    params = [
        CommitmentLevelProto::Processed,
        CommitmentLevelProto::Confirmed,
        CommitmentLevelProto::Finalized,
    ]
)]
pub async fn it_should_join_replay_and_live_stream(
    config: &RunConfig,
    commitment: CommitmentLevelProto,
) -> Result<()> {
    let until = JoinUntil::LiveBanks(JOIN_LIVE_BANKS_TO_CHECK);
    tokio::time::timeout(
        JOIN_TIMEOUT,
        join_replay_and_live_stream(config, commitment, JOIN_REPLAY_DEPTH, until),
    )
    .await
    .with_context(|| {
        format!("{commitment:?}: timed out before {JOIN_LIVE_BANKS_TO_CHECK} live banks")
    })?
}

/// Verifies a replay from one slot behind the commitment's tip loses no slot: every slot from
/// there to `CONTIGUOUS_SLOTS_PAST_TIP` slots past the processed tip at connect reaches the
/// client at `commitment`, each bank whole and in order, as [`it_should_join_replay_and_live_stream`]
/// checks.
///
/// # Can fail on a skipped slot
///
/// It assumes every slot in the range has a bank. A leader that skips its slot leaves that slot
/// with no bank at all, so it never reaches `commitment`, and this test fails listing it as
/// missing although nothing was lost. Re-run it, or check the slot is skipped on the cluster
/// (for example with `solana block <slot>`), before reading such a failure as data loss.
///
/// ```text
/// time ──────────────────────────────────────────────────────────────────────►
/// slots:    tip-1   tip   tip+1   tip+2   ...   tip+10       (tip: commitment's tip)
///                       L, R
/// replay:   tip-1   tip
/// live:                   tip+1   tip+2   ...   tip+10
/// client:   tip-1   tip   tip+1   tip+2   ...   tip+10       none missing, none twice
/// ```
#[test_helper(
    name = "replay-contiguous",
    tags = ["ordering", "replay"],
    params = [
        CommitmentLevelProto::Processed,
        CommitmentLevelProto::Confirmed,
        CommitmentLevelProto::Finalized,
    ]
)]
pub async fn it_should_replay_contiguous_slots_from_the_tip(
    config: &RunConfig,
    commitment: CommitmentLevelProto,
) -> Result<()> {
    let until = JoinUntil::ContiguousPastTip(CONTIGUOUS_SLOTS_PAST_TIP);
    tokio::time::timeout(
        JOIN_TIMEOUT,
        join_replay_and_live_stream(config, commitment, 1, until),
    )
    .await
    .with_context(|| {
        format!(
            "{commitment:?}: timed out before the slot {CONTIGUOUS_SLOTS_PAST_TIP} past the tip"
        )
    })?
}

/// Replays from `depth` slots behind the commitment's tip, then reads the live stream until
/// `until`, checking every bank as [`it_should_join_replay_and_live_stream`] describes.
async fn join_replay_and_live_stream(
    config: &RunConfig,
    commitment: CommitmentLevelProto,
    depth: u64,
    until: JoinUntil,
) -> Result<()> {
    let reached_status = match commitment {
        CommitmentLevelProto::Processed => SlotStatus::SlotProcessed,
        CommitmentLevelProto::Confirmed => SlotStatus::SlotConfirmed,
        CommitmentLevelProto::Finalized => SlotStatus::SlotFinalized,
    };
    const STATUS_ORDER: [SlotStatus; 3] = [
        SlotStatus::SlotProcessed,
        SlotStatus::SlotConfirmed,
        SlotStatus::SlotFinalized,
    ];

    let mut client = new_client(config).await?;
    // Banks up to `commitment_tip` were at `commitment` at connect, so they come from the
    // replay; banks from `processed_tip` on were not sealed yet, so they come live.
    let processed_tip = client
        .get_slot(Some(CommitmentLevelProto::Processed))
        .await
        .context("get_slot at processed")?
        .slot;
    let commitment_tip = client
        .get_slot(Some(commitment))
        .await
        .with_context(|| format!("get_slot at {commitment:?}"))?
        .slot;
    let from_slot = commitment_tip.saturating_sub(depth);
    let subscription = SubscribeRequest {
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]),
        accounts: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterAccounts {
                account: MUST_HAVE_SYSVAR_ACCOUNTS.map(str::to_string).to_vec(),
                ..Default::default()
            },
        )]),
        transactions: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterTransactions::default(),
        )]),
        entry: HashMap::from([("test".to_string(), Default::default())]),
        // Light blocks: only where the Block falls matters here.
        blocks: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterBlocks {
                include_accounts: Some(false),
                include_transactions: Some(false),
                include_entries: Some(false),
                ..Default::default()
            },
        )]),
        block_footer: HashMap::from([("test".to_string(), Default::default())]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        commitment: Some(commitment as i32),
        from_slot: Some(from_slot),
        ..Default::default()
    };
    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;
    log::info!(
        "{commitment:?}: processed tip {processed_tip}, {commitment:?} tip {commitment_tip}, \
         replaying from slot {from_slot}"
    );

    let mut banks: HashMap<(u64, u64), JoinedBank> = HashMap::new();
    let mut replayed_reached = 0usize;
    let mut live_reached = 0usize;
    // Slots with a bank that reached `commitment`.
    let mut reached_slots = HashSet::new();
    let finished = |live_reached: usize, reached_slots: &HashSet<u64>| match until {
        JoinUntil::LiveBanks(banks) => live_reached >= banks,
        JoinUntil::ContiguousPastTip(past) => reached_slots.contains(&(processed_tip + past)),
    };

    while !finished(live_reached, &reached_slots) {
        let update = stream
            .next()
            .await
            .context("stream ended")?
            .context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };
        match update_oneof {
            UpdateOneof::Slot(slot) => {
                let status = slot.status();
                // Lifecycle events that name no bank go out as they land, at every commitment.
                if matches!(
                    status,
                    SlotStatus::SlotFirstShredReceived
                        | SlotStatus::SlotCompleted
                        | SlotStatus::SlotDead
                ) {
                    continue;
                }
                if slot.slot < from_slot {
                    continue;
                }
                let bank_id = slot.bank_id.with_context(|| {
                    format!("slot {}: {status:?} should have a bank_id", slot.slot)
                })?;
                let bank = banks.entry((slot.slot, bank_id)).or_default();
                if status == SlotStatus::SlotCreatedBank {
                    ensure!(
                        !bank.created_bank,
                        "slot {} bank {bank_id}: CreatedBank arrived twice",
                        slot.slot
                    );
                    ensure!(
                        !bank.block_meta,
                        "slot {} bank {bank_id}: CreatedBank arrived after the block meta",
                        slot.slot
                    );
                    bank.created_bank = true;
                    continue;
                }
                ensure!(
                    bank.block_meta,
                    "slot {} bank {bank_id}: {status:?} arrived before the block meta",
                    slot.slot
                );
                let expected = STATUS_ORDER.get(bank.statuses.len());
                ensure!(
                    expected == Some(&status),
                    "slot {} bank {bank_id}: {status:?} after {:?}, expected {expected:?}",
                    slot.slot,
                    bank.statuses
                );
                bank.statuses.push(status);
                if status == reached_status {
                    reached_slots.insert(slot.slot);
                    if slot.slot <= commitment_tip {
                        replayed_reached += 1;
                    } else if slot.slot >= processed_tip {
                        live_reached += 1;
                    }
                }
            }
            UpdateOneof::Account(account) => {
                let Some(bank_id) = account.bank_id else {
                    continue;
                };
                if account.slot < from_slot {
                    continue;
                }
                let bank = banks.entry((account.slot, bank_id)).or_default();
                ensure!(
                    !bank.block,
                    "slot {} bank {bank_id}: an account arrived after the Block",
                    account.slot
                );
            }
            UpdateOneof::Transaction(transaction) => {
                if transaction.slot < from_slot {
                    continue;
                }
                let key = (transaction.slot, transaction.bank_id);
                let bank = banks.entry(key).or_default();
                ensure!(
                    !bank.block,
                    "slot {} bank {}: a transaction arrived after the Block",
                    key.0,
                    key.1
                );
                let signature = transaction
                    .transaction
                    .context("transaction update should have a transaction")?
                    .signature;
                ensure!(
                    bank.transactions.insert(signature),
                    "slot {} bank {}: a transaction arrived twice",
                    key.0,
                    key.1
                );
            }
            UpdateOneof::Entry(entry) => {
                if entry.slot < from_slot {
                    continue;
                }
                let key = (entry.slot, entry.bank_id);
                let bank = banks.entry(key).or_default();
                ensure!(
                    !bank.block,
                    "slot {} bank {}: an entry arrived after the Block",
                    key.0,
                    key.1
                );
                ensure!(
                    bank.entries.insert(entry.index),
                    "slot {} bank {}: entry {} arrived twice",
                    key.0,
                    key.1,
                    entry.index
                );
            }
            UpdateOneof::Block(block) => {
                if block.slot < from_slot {
                    continue;
                }
                let key = (block.slot, block.bank_id);
                let bank = banks.entry(key).or_default();
                ensure!(
                    !bank.block && !bank.footer && !bank.block_meta,
                    "slot {} bank {}: Block arrived twice, or after the footer or block meta",
                    key.0,
                    key.1
                );
                bank.block = true;
            }
            UpdateOneof::BlockFooter(footer) => {
                if footer.slot < from_slot {
                    continue;
                }
                let key = (footer.slot, footer.bank_id);
                let bank = banks.entry(key).or_default();
                ensure!(
                    bank.block && !bank.footer && !bank.block_meta,
                    "slot {} bank {}: footer arrived twice, before the Block or after the block meta",
                    key.0,
                    key.1
                );
                bank.footer = true;
            }
            UpdateOneof::BlockMeta(meta) => {
                if meta.slot < from_slot {
                    continue;
                }
                let key = (meta.slot, meta.bank_id);
                let bank = banks.entry(key).or_default();
                ensure!(
                    !bank.block_meta,
                    "slot {} bank {}: block meta arrived twice",
                    key.0,
                    key.1
                );
                ensure!(
                    bank.created_bank && bank.block,
                    "slot {} bank {}: block meta arrived before CreatedBank or the Block",
                    key.0,
                    key.1
                );
                ensure!(
                    bank.transactions.len() as u64 == meta.executed_transaction_count,
                    "slot {} bank {}: {} of {} transactions before the block meta",
                    key.0,
                    key.1,
                    bank.transactions.len(),
                    meta.executed_transaction_count
                );
                ensure!(
                    bank.entries.len() as u64 == meta.entries_count,
                    "slot {} bank {}: {} of {} entries before the block meta",
                    key.0,
                    key.1,
                    bank.entries.len(),
                    meta.entries_count
                );
                bank.block_meta = true;
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => {}
            other => bail!("unexpected update type: {other:?}"),
        }
    }

    ensure!(
        replayed_reached > 0,
        "{commitment:?}: no replayed bank, from slot {from_slot} to {commitment_tip}, reached {commitment:?}"
    );
    if let JoinUntil::ContiguousPastTip(past) = until {
        let missing: Vec<u64> = (from_slot..=processed_tip + past)
            .filter(|slot| !reached_slots.contains(slot))
            .collect();
        ensure!(
            missing.is_empty(),
            "{commitment:?}: slots from {from_slot} to {} that never reached {commitment:?} \
             (lost, or skipped by their leader): {missing:?}",
            processed_tip + past
        );
    }
    log::info!("{commitment:?}: {replayed_reached} replayed and {live_reached} live banks checked");
    Ok(())
}

/// Verifies that all sysvars are present in the updates.
/// This test subscribe by replaying from the last 200 slots.
/// at the processed commitment level.
#[test_helper(name = "all-sysvar-present", tags = ["replay"])]
pub async fn all_sysvar_must_be_present_in_replay(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;

    let resp = client.get_slot(None).await.context("get_slot")?;
    let tip = resp.slot;
    let musthave_pubkeys = must_have_sysvar_pubkeys()?;
    let account_filter = SubscribeRequestFilterAccounts {
        account: MUST_HAVE_SYSVAR_ACCOUNTS.map(str::to_string).to_vec(),
        ..Default::default()
    };
    let from_slot = tip.saturating_sub(200);
    let subscription = SubscribeRequest {
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(false),
                ..Default::default()
            },
        )]),
        accounts: HashMap::from([("test".to_string(), account_filter)]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        from_slot: Some(from_slot),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;
    log::info!(
        "current tip slot is {}, subscribing from slot {}",
        tip,
        from_slot
    );
    let mut remaining_slot_to_visit = Vec::from_iter(from_slot..tip);
    let mut block_meta_received = HashSet::new();
    let mut slot_status_received = HashMap::new();
    // Every must-have sysvar pubkey actually observed for a given slot.
    let mut sysvar_seen_per_slot: HashMap<u64, HashSet<Pubkey>> = HashMap::new();
    while let Some(update) = stream.next().await {
        if remaining_slot_to_visit.is_empty() {
            break;
        }
        let update = update.context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Slot(slot) => {
                slot_status_received.insert(slot.slot, slot.status());
                ensure!(
                    slot.parent.is_some(),
                    "slot update should have parent slot for replayed slots"
                );

                // A slot emits several lifecycle statuses (FirstShredReceived,
                // CreatedBank, Completed, ...) before Processed -- checking on
                // any of those would be premature, since sysvar/account
                // updates for the slot may not have arrived yet. Processed is
                // the first status that means "this slot's data is in".
                if slot.status() == SlotStatus::SlotProcessed
                    && remaining_slot_to_visit.contains(&slot.slot)
                {
                    // We mark dead slots like this as replay is currently not sending DeadSlot notifications even when intra slot updates are enabled
                    // Therefore we manually mark any skipped slots between the parent and the current slot as dead.
                    // In order for this e2e not to hang
                    if let Some(parent) = slot.parent {
                        if parent + 1 != slot.slot {
                            for dead_slot in parent + 1..slot.slot {
                                log::info!("marking slot {} as dead", dead_slot);
                                remaining_slot_to_visit.retain(|s| *s != dead_slot);
                                slot_status_received.insert(dead_slot, SlotStatus::SlotDead);
                            }
                        }
                    }

                    let seen = sysvar_seen_per_slot
                        .get(&slot.slot)
                        .cloned()
                        .unwrap_or_default();
                    let missing: Vec<Pubkey> = musthave_pubkeys
                        .iter()
                        .filter(|pk| !seen.contains(*pk))
                        .copied()
                        .collect();
                    ensure!(
                        missing.is_empty(),
                        "every must-have sysvar should be present for every slot, but slot {} was missing: {missing:?}",
                        slot.slot
                    );

                    remaining_slot_to_visit.retain(|s| *s != slot.slot);
                    log::info!("finished processing slot {}", slot.slot);
                }
            }
            UpdateOneof::Account(subscribe_update_account) => {
                let account = subscribe_update_account
                    .account
                    .context("account update should have account field")?;
                let actual_pubkey = Pubkey::try_from(account.pubkey.as_slice())
                    .map_err(|_| anyhow::anyhow!("invalid account pubkey bytes"))?;

                ensure!(
                    !block_meta_received.contains(&subscribe_update_account.slot),
                    "block meta should always be last to arrive for a slot, after all account updates have been received"
                );
                ensure!(
                    !slot_status_received.contains_key(&subscribe_update_account.slot),
                    "slot status should always be last to arrive for a slot, after all account updates have been received"
                );

                sysvar_seen_per_slot
                    .entry(subscribe_update_account.slot)
                    .or_default()
                    .insert(actual_pubkey);
            }
            UpdateOneof::BlockMeta(ev) => {
                block_meta_received.insert(ev.slot);
                ensure!(
                    !slot_status_received.contains_key(&ev.slot),
                    "slot status should always be last to arrive for a slot, after all block meta updates have been received"
                );
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => continue,
            other => bail!("unexpected update type: {other:?}"),
        }
    }
    ensure!(
        slot_status_received.len() == (tip - from_slot) as usize,
        "should receive slot status updates for all slots in the replay"
    );
    ensure!(
        remaining_slot_to_visit.is_empty(),
        "should have received updates for all expected slots in the replay"
    );

    Ok(())
}
