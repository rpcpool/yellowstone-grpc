use {
    crate::scenarios::RunConfig,
    anyhow::{ensure, Context, Result},
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        str::FromStr,
    },
    tokio::time::{timeout, Duration},
    tokio_stream::StreamExt,
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_proto::geyser::{
        subscribe_update_deshred, SlotStatus, SubscribeDeshredRequest,
        SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterSlots,
        SubscribeUpdateDeshredTransactionInfo,
    },
};

/// Validates deshred subscription flow and deshredded output handling.
#[test_helper(name = "deshred", tags = ["deshred"])]
pub async fn test_subscribe_deshred(config: &RunConfig) -> Result<()> {
    let mut client = crate::grpc::new_client(config).await?;

    let subscription = SubscribeDeshredRequest {
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]),
        deshred_transactions: HashMap::from([("test".to_string(), Default::default())]),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_deshred_once(subscription)
        .await
        .context("subscription should succeed")?;

    let mut deshred_txn_count = 0;

    let mut remaining_slot_lifecycle_to_visit = Vec::from_iter([
        SlotStatus::SlotCompleted,
        SlotStatus::SlotConfirmed,
        SlotStatus::SlotFinalized,
        SlotStatus::SlotFirstShredReceived,
        SlotStatus::SlotCreatedBank,
        SlotStatus::SlotProcessed,
    ]);

    let mut block_visit = HashSet::new();
    const BLOCK_TO_VISIT: usize = 32;
    while let Some(update) = stream.next().await {
        if block_visit.len() >= BLOCK_TO_VISIT || remaining_slot_lifecycle_to_visit.is_empty() {
            break;
        }
        let update = update.context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            subscribe_update_deshred::UpdateOneof::DeshredTransaction(
                subscribe_update_deshred_transaction,
            ) => {
                let slot = subscribe_update_deshred_transaction.slot;
                block_visit.insert(slot);
                deshred_txn_count += 1;
                subscribe_update_deshred_transaction
                    .transaction
                    .context("deshred transaction update should have transaction field")?;
            }
            subscribe_update_deshred::UpdateOneof::Slot(subscribe_update_slot) => {
                block_visit.insert(subscribe_update_slot.slot);
                let status = subscribe_update_slot.status();
                log::info!(
                    "received slot update for slot {} with status {:?}",
                    subscribe_update_slot.slot,
                    status
                );
                if let Some(pos) = remaining_slot_lifecycle_to_visit
                    .iter()
                    .position(|&s| s == status)
                {
                    remaining_slot_lifecycle_to_visit.remove(pos);
                }
            }
            _ => {}
        }
    }
    ensure!(
        deshred_txn_count > 0,
        "should receive at least one deshred transaction update"
    );
    ensure!(
        remaining_slot_lifecycle_to_visit.is_empty(),
        "should have received updates for all expected slot lifecycle in the replay. missing: {:?}",
        remaining_slot_lifecycle_to_visit,
    );

    Ok(())
}

/// Verifies that deshred address lookup table (ALT) is properly resolved and that account_include filters match on ALT-resolved addresses.
#[test_helper(name = "deshred-alt-resolution", tags = ["deshred", "alt", "filters"])]
pub async fn deshred_should_resolve_and_filter_address_lookup_tables(
    config: &RunConfig,
) -> Result<()> {
    // Verifies deshred address lookup table (ALT) support end to end, filtering on a list of
    // common accounts. In one pass it checks that ALT resolution populates the loaded
    // writable/readonly addresses to match the lookup index counts, and that an `account_include`
    // filter selects transactions where a filtered account is present only in the ALT-loaded
    // addresses (not the static keys), proving the filter matches on ALT-resolved accounts. Both
    // checks fail when resolution is disabled.

    const FILTER_ACCOUNTS: [&str; 5] = [
        "11111111111111111111111111111111",             // System Program
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  // SPL Token program
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC mint
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", // USDT mint
        "So11111111111111111111111111111111111111112",  // wSOL mint
    ];
    let filter_accounts: HashSet<Pubkey> = FILTER_ACCOUNTS
        .iter()
        .map(|account| Pubkey::from_str(account).expect("valid pubkey"))
        .collect();

    let mut client = crate::grpc::new_client(config).await?;
    let subscription = SubscribeDeshredRequest {
        deshred_transactions: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                account_include: FILTER_ACCOUNTS.iter().map(|a| a.to_string()).collect(),
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    let mut stream = client
        .subscribe_deshred_once(subscription)
        .await
        .context("subscription should succeed")?;

    const NEXT_UPDATE_TIMEOUT: Duration = Duration::from_secs(30);
    const MAX_DESHRED_TXNS: usize = 5_000;
    const MIN_ALT_TXNS_RESOLVED: usize = 3;

    let mut scanned = 0usize;
    // Delivered transactions that reference at least one address lookup table.
    let mut alt_seen = 0usize;
    // ...of those, the ones whose resolved addresses match the lookup index counts.
    let mut alt_resolved = 0usize;
    // Delivered transactions matched by a filtered account present only in ALT-loaded addresses.
    let mut matched_via_alt = 0usize;

    while scanned < MAX_DESHRED_TXNS
        && (alt_resolved < MIN_ALT_TXNS_RESOLVED || matched_via_alt == 0)
    {
        let Ok(Some(update)) = timeout(NEXT_UPDATE_TIMEOUT, stream.next()).await else {
            break;
        };
        let update = update.context("stream should yield updates without error")?;
        let Some(subscribe_update_deshred::UpdateOneof::DeshredTransaction(deshred)) =
            update.update_oneof
        else {
            continue;
        };
        scanned += 1;

        let slot = deshred.slot;
        let info = deshred
            .transaction
            .context("deshred transaction update should have transaction field")?;
        let message = info
            .transaction
            .as_ref()
            .and_then(|tx| tx.message.as_ref())
            .context("deshred transaction should carry a decoded message")?;

        let expected_writable: usize = message
            .address_table_lookups
            .iter()
            .map(|lookup| lookup.writable_indexes.len())
            .sum();
        let expected_readonly: usize = message
            .address_table_lookups
            .iter()
            .map(|lookup| lookup.readonly_indexes.len())
            .sum();

        let (static_keys, loaded) = deshred_static_and_loaded_keys(&info);

        // account_include must only deliver transactions that reference one of the accounts.
        ensure!(
            filter_accounts
                .iter()
                .any(|account| static_keys.contains(account) || loaded.contains(account)),
            "slot {slot}: filter delivered a deshred transaction containing none of the filtered accounts"
        );

        // When the transaction references lookup tables, resolution must be populated and
        // match the index counts. Deshred is pre-commitment and ALTs resolve against the
        // rooted bank, so a table not yet rooted occasionally resolves to nothing; we
        // therefore require a minimum number of fully resolved transactions rather than
        // failing on the first miss. When resolution is disabled every such transaction
        // comes back empty and the minimum is never reached.
        if expected_writable + expected_readonly > 0 {
            alt_seen += 1;
            if info.loaded_writable_addresses.len() == expected_writable
                && info.loaded_readonly_addresses.len() == expected_readonly
            {
                alt_resolved += 1;
            } else {
                log::warn!(
                    "slot {slot}: ALT lookups expected {expected_writable} writable / {expected_readonly} readonly, got {} / {}",
                    info.loaded_writable_addresses.len(),
                    info.loaded_readonly_addresses.len(),
                );
            }
        }

        // A single filtered account present only in the ALT-loaded addresses (not the static
        // keys) is enough to prove the filter matched via ALT resolution.
        if let Some(account) = filter_accounts
            .iter()
            .find(|account| loaded.contains(account) && !static_keys.contains(account))
        {
            matched_via_alt += 1;
            log::info!(
                "slot {slot}: filtered account {account} matched via ALT-loaded addresses only"
            );
        }
    }

    ensure!(
        alt_seen > 0,
        "scanned {scanned} deshred transactions but none referenced an address lookup table, cannot verify ALT resolution"
    );
    ensure!(
        alt_resolved >= MIN_ALT_TXNS_RESOLVED,
        "resolved ALT addresses on only {alt_resolved} of {alt_seen} deshred transactions carrying lookups (needed {MIN_ALT_TXNS_RESOLVED}); ALT resolution is likely disabled on the endpoint (deshred_transaction_alt_resolution_enabled)"
    );
    ensure!(
        matched_via_alt > 0,
        "account_include filter never matched a deshred transaction where a filtered account was present only in ALT-loaded addresses after scanning {scanned} transactions; the filter may not be considering ALT-resolved addresses"
    );

    Ok(())
}

/// Splits a deshred transaction's account keys into (static keys, ALT-loaded addresses).
fn deshred_static_and_loaded_keys(
    info: &SubscribeUpdateDeshredTransactionInfo,
) -> (HashSet<Pubkey>, HashSet<Pubkey>) {
    let static_keys = info
        .transaction
        .as_ref()
        .and_then(|tx| tx.message.as_ref())
        .map(|message| {
            message
                .account_keys
                .iter()
                .filter_map(|key| Pubkey::try_from(key.as_slice()).ok())
                .collect()
        })
        .unwrap_or_default();
    let loaded = info
        .loaded_writable_addresses
        .iter()
        .chain(info.loaded_readonly_addresses.iter())
        .filter_map(|addr| Pubkey::try_from(addr.as_slice()).ok())
        .collect();
    (static_keys, loaded)
}
