use {
    crate::{grpc::new_client, scenarios::RunConfig},
    anyhow::{bail, ensure, Context, Result},
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

/// Verifies replay support by receiving historical data from a replay request.
/// Each replayed bank's CreatedBank comes before its block meta, and before its accounts other
/// than the must-have sysvars and epoch-crossing accounts written while the bank is
/// constructed, as in the live stream.
/// Each replayed bank brings every must-have sysvar account.
/// At processed, each bank must also bring all its transactions before its block meta.
#[test_helper(name = "replay", tags = ["replay"])]
pub async fn it_should_support_replay(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;

    let resp = client.get_slot(None).await.context("get_slot")?;
    let tip = resp.slot;
    let musthave_pubkeys = must_have_sysvar_pubkeys()?;
    let before_created_bank = before_created_bank_pubkeys()?;
    let account_filter = SubscribeRequestFilterAccounts {
        account: MUST_HAVE_SYSVAR_ACCOUNTS.map(str::to_string).to_vec(),
        ..Default::default()
    };
    let from_slot = tip.saturating_sub(10);
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
        from_slot: Some(from_slot),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;
    let mut count = 0usize;
    const MAX_UPDATES: usize = 10;
    log::info!(
        "current tip slot is {}, subscribing from slot {}",
        tip,
        from_slot
    );
    let mut remaining_slot_to_visit = Vec::from_iter(from_slot..tip);
    let mut block_visited = HashSet::new();
    let mut block_meta_received = HashSet::new();
    let mut slot_status_received = HashMap::new();
    // Keyed by (slot, bank_id): a slot can have more than one bank across forks.
    let mut created_banks = HashSet::new();
    let mut musthave_seen: HashMap<(u64, u64), HashSet<Pubkey>> = HashMap::new();
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
                slot_status_received.insert(slot.slot, slot.status());
                if block_visited.contains(&slot.slot) {
                    count += 1;
                }
                remaining_slot_to_visit.retain(|s| *s != slot.slot);
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
        slot_status_received.len() == (tip - from_slot) as usize,
        "should receive slot status updates for all slots in the replay"
    );
    ensure!(
        remaining_slot_to_visit.is_empty(),
        "should have received updates for all expected slots in the replay"
    );
    let created_bank_slots: HashSet<u64> = created_banks.iter().map(|(slot, _)| *slot).collect();
    let slots_without_created_bank: Vec<u64> = (from_slot..tip)
        .filter(|slot| !created_bank_slots.contains(slot))
        .collect();
    ensure!(
        slots_without_created_bank.is_empty(),
        "should receive CreatedBank for every slot in the replay, missing for slots {slots_without_created_bank:?}"
    );
    let mut banks_missing_musthave: Vec<(u64, u64, Vec<Pubkey>)> = created_banks
        .iter()
        .filter(|(slot, _)| (from_slot..tip).contains(slot))
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

/// Verifies replay message ordering matches the live broadcast path:
/// block data (Account/Transaction/Entry) before Block before BlockMeta before slot status.
#[test_helper(name = "replay-ordering", tags = ["ordering", "replay"])]
pub async fn it_should_verify_replay_ordering_matches_live_path(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;

    let resp = client.get_slot(None).await.context("get_slot")?;
    let tip = resp.slot;
    let from_slot = tip.saturating_sub(10);

    let subscription = SubscribeRequest {
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]),
        blocks: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterBlocks {
                include_accounts: Some(true),
                include_transactions: Some(true),
                include_entries: Some(true),
                ..Default::default()
            },
        )]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        accounts: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                ..Default::default()
            },
        )]),
        transactions: HashMap::from([("test".to_string(), Default::default())]),
        entry: HashMap::from([("test".to_string(), Default::default())]),
        from_slot: Some(from_slot),
        commitment: Some(1),
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

    // Track message ordering per slot
    #[derive(Default)]
    struct SlotOrdering {
        last_data_seq: Option<usize>,
        block_seq: Option<usize>,
        blockmeta_seq: Option<usize>,
        first_status_seq: Option<usize>,
    }

    let mut slot_orderings: HashMap<u64, SlotOrdering> = HashMap::new();
    let mut seq: usize = 0;
    let mut validated_slots = 0usize;
    const TARGET_VALIDATED_SLOTS: usize = 3;

    while let Some(update) = stream.next().await {
        if validated_slots >= TARGET_VALIDATED_SLOTS {
            break;
        }
        let update = update.context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Account(ev) => {
                let ordering = slot_orderings.entry(ev.slot).or_default();
                ordering.last_data_seq = Some(seq);
                seq += 1;
            }
            UpdateOneof::Transaction(ev) => {
                let ordering = slot_orderings.entry(ev.slot).or_default();
                ordering.last_data_seq = Some(seq);
                seq += 1;
            }
            UpdateOneof::Entry(ev) => {
                let ordering = slot_orderings.entry(ev.slot).or_default();
                ordering.last_data_seq = Some(seq);
                seq += 1;
            }
            UpdateOneof::Block(ev) => {
                let ordering = slot_orderings.entry(ev.slot).or_default();
                ordering.block_seq = Some(seq);
                seq += 1;
            }
            UpdateOneof::BlockMeta(ev) => {
                let ordering = slot_orderings.entry(ev.slot).or_default();
                ordering.blockmeta_seq = Some(seq);
                seq += 1;

                // BlockMeta is the last content message before slot status.
                // Validate ordering for this slot if we have all phases.
                let ordering = slot_orderings.get(&ev.slot).unwrap();
                if let (Some(last_data), Some(block), Some(blockmeta)) = (
                    ordering.last_data_seq,
                    ordering.block_seq,
                    ordering.blockmeta_seq,
                ) {
                    ensure!(
                        last_data < block,
                        "slot {}: block data (seq {last_data}) must arrive before Block (seq {block})",
                        ev.slot
                    );
                    ensure!(
                        block < blockmeta,
                        "slot {}: Block (seq {block}) must arrive before BlockMeta (seq {blockmeta})",
                        ev.slot
                    );
                    validated_slots += 1;
                    log::info!(
                        "slot {}: ordering verified {validated_slots}/{TARGET_VALIDATED_SLOTS}",
                        ev.slot
                    );
                }
            }
            UpdateOneof::Slot(ev) => {
                let ordering = slot_orderings.entry(ev.slot).or_default();
                if ordering.first_status_seq.is_none() {
                    ordering.first_status_seq = Some(seq);

                    // If we already have blockmeta, verify status comes after
                    if let Some(blockmeta) = ordering.blockmeta_seq {
                        ensure!(
                            blockmeta < seq,
                            "slot {}: BlockMeta (seq {blockmeta}) must arrive before slot status (seq {seq})",
                            ev.slot
                        );
                    }
                }
                seq += 1;
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => continue,
            _ => continue,
        }
    }

    ensure!(
        validated_slots >= TARGET_VALIDATED_SLOTS,
        "should have validated ordering for at least {TARGET_VALIDATED_SLOTS} slots, got {validated_slots}"
    );

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
