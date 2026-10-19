use {
    super::block_footer::{check_footer_fields, footer_and_block_meta_request, UPDATE_TIMEOUT},
    crate::{grpc::new_client, scenarios::RunConfig},
    anyhow::{bail, ensure, Context, Result},
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        str::FromStr,
        time::Duration,
    },
    tokio::time::timeout,
    tokio_stream::StreamExt,
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto, SlotStatus,
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    },
};

/// Verifies replay support by receiving historical data from a replay request.
/// Each replayed bank's CreatedBank comes before its accounts and block meta.
/// At processed, each bank must also bring all its transactions before its block meta.
#[test_helper(name = "replay", tags = ["replay"])]
pub async fn it_should_support_replay(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;

    let resp = client.get_slot(None).await.context("get_slot")?;
    let tip = resp.slot;
    let sysvar_clock_str = "SysvarC1ock11111111111111111111111111111111";
    let sysvar_clock_pubkey = Pubkey::from_str(sysvar_clock_str).context("valid pubkey string")?;
    let account_filter = SubscribeRequestFilterAccounts {
        account: vec![sysvar_clock_str.to_string()],
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
                    actual_pubkey == sysvar_clock_pubkey,
                    "received unexpected pubkey"
                );
                block_visited.insert(subscribe_update_account.slot);

                let bank_id = subscribe_update_account
                    .bank_id
                    .context("replayed account update should have a bank_id")?;
                ensure!(
                    subscribe_update_account.slot >= tip
                        || created_banks.contains(&(subscribe_update_account.slot, bank_id)),
                    "slot {} bank_id {bank_id}: CreatedBank should arrive before the bank's account updates",
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
    let sysvar_account_musthave = vec![
        // emitted before CreatedBank
        "SysvarC1ock11111111111111111111111111111111".to_string(),
        "SysvarS1otHashes111111111111111111111111111".to_string(),
        // written when the block freeze
        "SysvarS1otHistory11111111111111111111111111".to_string(),
        "SysvarRecentB1ockHashes11111111111111111111".to_string(),
    ];
    let musthave_pubkeys = sysvar_account_musthave
        .iter()
        .map(|s| Pubkey::from_str(s).context("valid must-have sysvar pubkey string"))
        .collect::<Result<HashSet<_>>>()?;
    let account_filter = SubscribeRequestFilterAccounts {
        account: sysvar_account_musthave,
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

/// Verifies `from_slot` replay sends each bank's block footer before its block meta.
#[test_helper(name = "block-footer-replay", tags = ["block-footer", "alpenglow", "replay"])]
pub async fn block_footer_should_replay_before_block_meta(config: &RunConfig) -> Result<()> {
    const REPLAY_DEPTH: u64 = 10;
    // Slots skipped by their leader have no block, so require only some replayed banks.
    const MIN_REPLAYED_BANKS: usize = 3;

    let mut client = crate::grpc::new_client(config).await?;
    let tip = client.get_slot(None).await.context("get_slot")?.slot;
    let from_slot = tip.saturating_sub(REPLAY_DEPTH);
    log::info!("current tip slot is {tip}, replaying from slot {from_slot}");

    let mut stream = client
        .subscribe_once(footer_and_block_meta_request(
            CommitmentLevelProto::Processed,
            Some(from_slot),
        ))
        .await
        .context("subscription should succeed")?;

    let mut footers: HashSet<(u64, u64)> = HashSet::new();
    let mut replayed_banks = 0usize;
    loop {
        let update = timeout(UPDATE_TIMEOUT, stream.next())
            .await
            .with_context(|| format!("no update within {UPDATE_TIMEOUT:?}"))?
            .context("stream ended before replay caught up with the tip")?
            .context("stream should yield updates without error")?;

        match update.update_oneof {
            Some(UpdateOneof::BlockFooter(footer)) => {
                check_footer_fields(&footer)?;
                ensure!(
                    footers.insert((footer.slot, footer.bank_id)),
                    "received duplicate footer for slot {} bank_id {}",
                    footer.slot,
                    footer.bank_id
                );
            }
            Some(UpdateOneof::BlockMeta(block_meta)) => {
                if block_meta.slot >= tip {
                    break;
                }
                ensure!(
                    block_meta.slot >= from_slot,
                    "slot {}: replay sent a block meta before from_slot {from_slot}",
                    block_meta.slot
                );
                ensure!(
                    footers.contains(&(block_meta.slot, block_meta.bank_id)),
                    "slot {} bank_id {}: replayed block meta arrived without a footer before it",
                    block_meta.slot,
                    block_meta.bank_id
                );
                replayed_banks += 1;
            }
            _ => {}
        }
    }

    ensure!(
        replayed_banks >= MIN_REPLAYED_BANKS,
        "only {replayed_banks} banks replayed between slot {from_slot} and tip {tip}, expected at least {MIN_REPLAYED_BANKS}"
    );
    Ok(())
}
