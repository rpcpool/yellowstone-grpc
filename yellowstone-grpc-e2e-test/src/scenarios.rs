//! E2E subscription scenarios and discovery metadata for the test CLI.

use {
    anyhow::{bail, ensure, Context, Result},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::{HashMap, HashSet},
        str::FromStr,
        sync::Once,
    },
    tokio_stream::StreamExt,
    yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient},
    yellowstone_grpc_e2e_macros::test_helper,
    yellowstone_grpc_geyser::plugin::message::CommitmentLevel,
    yellowstone_grpc_proto::geyser::{
        subscribe_request_filter_accounts_filter::Filter, subscribe_update::UpdateOneof,
        subscribe_update_deshred, SlotStatus, SubscribeDeshredRequest, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeUpdateAccount, SubscribeUpdateBlock,
        SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateTransaction,
        TokenAccountExpansionControlFlag,
    },
};

static LOG_INIT: Once = Once::new();

pub struct ScenarioDoc {
    /// Stable CLI scenario name (for example `sysvar-account`).
    pub name: &'static str,
    /// Human-friendly scenario description shown in `yellowstone-e2e list`.
    pub description: &'static str,
}

inventory::collect!(ScenarioDoc);

/// Returns the registered scenario description for a CLI scenario name.
pub fn scenario_description(name: &str) -> Option<&'static str> {
    inventory::iter::<ScenarioDoc>
        .into_iter()
        .find_map(|scenario| (scenario.name == name).then_some(scenario.description))
}

#[derive(Debug, Clone)]
pub struct RunConfig {
    /// gRPC endpoint URI used by scenarios.
    pub endpoint: String,
    /// Optional x-token used for authenticated requests.
    pub x_token: Option<String>,
}

/// Initializes logger once for scenario execution.
pub fn init_log() {
    LOG_INIT.call_once(|| {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error"))
            .try_init()
            .ok();
    });
}

async fn new_client(config: &RunConfig) -> Result<GeyserGrpcClient> {
    let mut builder = GeyserGrpcClient::build_from_shared(config.endpoint.clone())
        .context("endpoint should be a valid URI")?;

    if config.endpoint.starts_with("https://") {
        builder = builder
            .tls_config(ClientTlsConfig::new().with_enabled_roots())
            .context("failed to configure TLS for HTTPS endpoint")?;
    }

    let builder = builder
        .x_token(config.x_token.clone())
        .context("x-token should be valid ASCII metadata if provided")?;

    builder
        .max_decoding_message_size(100_000_000)
        .http2_adaptive_window(true)
        .accept_compressed(yellowstone_grpc_proto::tonic::codec::CompressionEncoding::Zstd)
        .connect()
        .await
        .context("client should build from endpoint and token")
}

/// Subscribes to account updates and verifies only SysvarClock updates are returned.
#[test_helper(name = "sysvar-account")]
pub async fn subscribe_should_only_returns_sysvarclock_account(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;
    let sysvar_clock_str = "SysvarC1ock11111111111111111111111111111111";
    let sysvar_clock_pubkey = Pubkey::from_str(sysvar_clock_str).context("valid pubkey string")?;
    let account_filter = SubscribeRequestFilterAccounts {
        account: vec![sysvar_clock_str.to_string()],
        ..Default::default()
    };
    let subscription = SubscribeRequest {
        accounts: HashMap::from([("test".to_string(), account_filter)]),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;
    let mut count = 0usize;
    const MAX_UPDATES: usize = 3;
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }
        let update = update.context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
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
                count += 1;
                log::info!(
                    "received account update for slot {} {count}/{MAX_UPDATES}",
                    subscribe_update_account.slot
                );
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => continue,
            other => bail!("unexpected update type: {other:?}"),
        }
    }

    Ok(())
}

/// Subscribes to blocks and verifies updates include changes touching SysvarClock.
#[test_helper(name = "sysvar-block")]
pub async fn subscribe_should_receive_block_where_sysvarclock1111_account_has_been_updated(
    config: &RunConfig,
) -> Result<()> {
    let mut client = new_client(config).await?;
    let sysvar_clock_str = "SysvarC1ock11111111111111111111111111111111";
    let sysvar_clock_pubkey = Pubkey::from_str(sysvar_clock_str).context("valid pubkey string")?;
    let block_filter = SubscribeRequestFilterBlocks {
        account_include: vec![sysvar_clock_str.to_string()],
        ..Default::default()
    };
    let subscription = SubscribeRequest {
        blocks: HashMap::from([("test".to_string(), block_filter)]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        commitment: Some(2),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;
    let mut count = 0usize;
    const MAX_UPDATES: usize = 3;

    let mut block_received = HashMap::new();
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }
        let update = update.context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Block(block) => {
                log::info!("received block update for slot {}", block.slot);
                for account in &block.accounts {
                    let actual_pubkey = Pubkey::try_from(account.pubkey.clone())
                        .map_err(|_| anyhow::anyhow!("invalid account pubkey bytes"))?;
                    ensure!(
                        actual_pubkey == sysvar_clock_pubkey,
                        "received non-sysvar account"
                    );
                }

                let blockhash = block.blockhash.clone();
                if let Some(blockmeta_blockhash) =
                    block_received.insert(block.slot, block.blockhash)
                {
                    ensure!(
                        blockhash == blockmeta_blockhash,
                        "blockhash in block should match block meta update"
                    );
                    count += 1;
                }
            }
            UpdateOneof::BlockMeta(meta) => {
                log::info!("received block meta update for slot {}", meta.slot);
                let blockhash = meta.blockhash.clone();
                if let Some(block_blockhash) = block_received.insert(meta.slot, meta.blockhash) {
                    ensure!(
                        blockhash == block_blockhash,
                        "blockhash in meta should match block update"
                    );
                    count += 1;
                }
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => continue,
            other => bail!("unexpected update type: {other:?}"),
        }
    }

    Ok(())
}

/// Subscribes to full block stream and validates full block payload delivery.
#[test_helper(name = "full-blocks")]
pub async fn subscribe_should_receive_full_blocks(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;

    let block_filter = SubscribeRequestFilterBlocks {
        include_accounts: Some(true),
        include_transactions: Some(true),
        include_entries: Some(true),
        ..Default::default()
    };
    let subscription = SubscribeRequest {
        blocks: HashMap::from([("test".to_string(), block_filter)]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        commitment: Some(1),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;
    const MAX_UPDATES: usize = 5;

    let mut block_received: HashMap<u64, (String, usize)> = HashMap::new();
    let mut block_meta_received: HashMap<u64, (String, u64)> = HashMap::new();
    let mut count = 0;
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }
        let update = update.context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Block(block) => {
                ensure!(
                    !block.accounts.is_empty(),
                    "should receive accounts for blocks"
                );
                ensure!(
                    !block.transactions.is_empty(),
                    "should receive transactions for blocks"
                );
                ensure!(
                    !block.entries.is_empty(),
                    "should receive entries for blocks"
                );
                ensure!(
                    block.executed_transaction_count == block.transactions.len() as u64,
                    "executed transaction count should match number of transactions"
                );
                let blockhash = block.blockhash.clone();
                block_received.insert(block.slot, (blockhash.clone(), block.transactions.len()));
            }
            UpdateOneof::BlockMeta(meta) => {
                let blockhash = meta.blockhash.clone();
                block_meta_received.insert(
                    meta.slot,
                    (blockhash.clone(), meta.executed_transaction_count),
                );

                if let Some((block_blockhash, actual_txn_cnt)) = block_received.get(&meta.slot) {
                    ensure!(
                        blockhash == *block_blockhash,
                        "blockhash in meta should match block update"
                    );
                    ensure!(
                        meta.executed_transaction_count as usize == *actual_txn_cnt,
                        "executed transaction count in meta should match number of transactions in block"  
                    );
                    count += 1;
                    log::info!(
                        "received block update for slot {} {count}/{MAX_UPDATES}",
                        meta.slot
                    );
                }
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => continue,
            other => bail!("unexpected update type: {other:?}"),
        }
    }

    Ok(())
}

/// Verifies replay support by receiving historical data from a replay request.
#[test_helper(name = "replay")]
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
    let mut slot_status_received = HashMap::new();
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }
        let update = update.context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Slot(slot) => {
                slot_status_received.insert(slot.slot, slot.status());
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
                count += 1;
                remaining_slot_to_visit.retain(|&slot| slot != subscribe_update_account.slot);
                log::info!(
                    "received account update for slot {} {count}/{MAX_UPDATES}",
                    subscribe_update_account.slot
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

/// Validates deshred subscription flow and deshredded output handling.
#[test_helper(name = "deshred")]
pub async fn test_subscribe_deshred(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;

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

/// Insure subscription at any commitment level returns all possible updates for that commitment, including all slot lifecycle updates, account updates, transaction updates and entry updates.
#[test_helper(name = "any-commitment")]
pub async fn any_commitment_level_of_subscription_should_return_all_possible_values(
    config: &RunConfig,
) -> Result<()> {
    let mut client = new_client(config).await?;
    let sysvar_clock_str = "SysvarC1ock11111111111111111111111111111111";

    for commitment in [0, 1, 2] {
        let account_filter = SubscribeRequestFilterAccounts {
            account: vec![sysvar_clock_str.to_string()],
            ..Default::default()
        };
        let subscription = SubscribeRequest {
            slots: HashMap::from([(
                "test".to_string(),
                SubscribeRequestFilterSlots {
                    interslot_updates: Some(true),
                    ..Default::default()
                },
            )]),
            blocks: HashMap::from([("test".to_string(), Default::default())]),
            entry: HashMap::from([("test".to_string(), Default::default())]),
            transactions: HashMap::from([("test".to_string(), Default::default())]),
            blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
            accounts: HashMap::from([("test".to_string(), account_filter)]),
            commitment: Some(commitment),
            ..Default::default()
        };

        let mut stream = client
            .subscribe_once(subscription)
            .await
            .context("subscription should succeed")?;

        let mut remaining_slot_lifecycle_to_visit = Vec::from_iter([
            SlotStatus::SlotCompleted,
            SlotStatus::SlotConfirmed,
            SlotStatus::SlotFinalized,
            SlotStatus::SlotFirstShredReceived,
            SlotStatus::SlotCreatedBank,
            SlotStatus::SlotProcessed,
        ]);

        let mut block_visited = HashSet::new();
        const MAX_UPDATES: usize = 32;
        let mut received_account_update = false;
        let mut rececived_txn_update = false;
        let mut received_entry = false;
        let mut received_blockmeta = false;
        let mut received_block = false;

        while let Some(update) = stream.next().await {
            if block_visited.len() >= MAX_UPDATES
                || (remaining_slot_lifecycle_to_visit.is_empty()
                    && received_account_update
                    && rececived_txn_update
                    && received_entry
                    && received_blockmeta
                    && received_block)
            {
                break;
            }
            let update = update.context("stream should yield updates without error")?;
            let Some(update_oneof) = update.update_oneof else {
                continue;
            };
            match update_oneof {
                UpdateOneof::Slot(slot) => {
                    block_visited.insert(slot.slot);
                    let status = slot.status();
                    log::info!(
                        "received slot update for slot {} with status {:?} for commitment level {commitment}",
                        slot.slot,
                        status
                    );
                    if let Some(pos) = remaining_slot_lifecycle_to_visit
                        .iter()
                        .position(|&s| s == status)
                    {
                        remaining_slot_lifecycle_to_visit.remove(pos);
                    }
                }
                UpdateOneof::Account(_) => {
                    received_account_update = true;
                }
                UpdateOneof::Transaction(_) => {
                    rececived_txn_update = true;
                }
                UpdateOneof::Entry(_) => {
                    received_entry = true;
                }
                UpdateOneof::BlockMeta(_) => {
                    received_blockmeta = true;
                }
                UpdateOneof::Block(_) => {
                    received_block = true;
                }
                _ => {}
            }
        }
        ensure!(
            remaining_slot_lifecycle_to_visit.is_empty(),
            "should have received updates for all expected slot lifecycle in the replay for commitment level {commitment}. missing: {:?}",
            remaining_slot_lifecycle_to_visit,
        );
        ensure!(
            received_account_update,
            "should receive account update for commitment level {commitment}"
        );
        ensure!(
            rececived_txn_update,
            "should receive transaction update for commitment level {commitment}"
        );
        ensure!(
            received_entry,
            "should receive entry update for commitment level {commitment}"
        );
    }

    Ok(())
}

/// Ensures token ATA activity for an owner is observed in transaction subscriptions.
#[test_helper(name = "token-owner-balance-changed")]
pub async fn it_should_subscribe_to_all_transaction_include_token_ata_to_an_owner(
    config: &RunConfig,
) -> Result<()> {
    let mut client = new_client(config).await?;
    const TOKEN_ACCOUNT_OWNER: &str = "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV";
    let bisonfi_token_owner =
        Pubkey::from_str(TOKEN_ACCOUNT_OWNER).context("valid pubkey string")?;

    let subscription = SubscribeRequest {
        transactions: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterTransactions {
                account_include: vec![TOKEN_ACCOUNT_OWNER.to_string()],
                token_accounts: Some(TokenAccountExpansionControlFlag::BalanceChanged as i32),
                ..Default::default()
            },
        )]),
        commitment: Some(0),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;

    let mut count = 0usize;
    const MAX_UPDATES: usize = 1;
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }

        let update = update.context("stream should yield updates without error")?;
        if let Some(UpdateOneof::Transaction(subscribe_update_transaction)) = update.update_oneof {
            let transaction = subscribe_update_transaction
                .transaction
                .context("transaction update should have transaction field")?;
            let meta = transaction
                .meta
                .context("transaction update should have meta")?;

            let in_post_balance = meta.post_token_balances.iter().any(|b| {
                Pubkey::from_str(&b.owner)
                    .map(|actual_pubkey| actual_pubkey == bisonfi_token_owner)
                    .unwrap_or(false)
            });
            let in_pre_balance = meta.pre_token_balances.iter().any(|b| {
                Pubkey::from_str(&b.owner)
                    .map(|actual_pubkey| actual_pubkey == bisonfi_token_owner)
                    .unwrap_or(false)
            });

            if in_post_balance && in_pre_balance {
                log::info!(
                    "received transaction update with token balance change for account {TOKEN_ACCOUNT_OWNER}"
                );
                count += 1;
            } else {
                log::info!(
                    "received transaction update but it does not have token balance change for account {TOKEN_ACCOUNT_OWNER}, pre balance has account: {in_pre_balance}, post balance has account: {in_post_balance}"
                );
            }
        }
    }

    ensure!(
        count > 0,
        "should receive at least one matching token balance-changed transaction update"
    );
    Ok(())
}

/// validators message ordering guarantees that are provided by geyser.
#[test_helper(name = "event-ordering")]
pub async fn it_should_verifies_geyser_event_ordering_is_correct(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;
    let subscription = SubscribeRequest {
        transactions: HashMap::from([("test".to_string(), Default::default())]),
        blocks: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterBlocks {
                include_accounts: Some(false),
                include_transactions: Some(false),
                include_entries: Some(false),
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
        entry: HashMap::from([("test".to_string(), Default::default())]),
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(0),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;

    struct BlockBuffer {
        slot: u64,
        account: Vec<SubscribeUpdateAccount>,
        transaction: Vec<SubscribeUpdateTransaction>,
        entry: Vec<SubscribeUpdateEntry>,
        block: Option<SubscribeUpdateBlock>,
        blockmeta: Option<SubscribeUpdateBlockMeta>,
    }

    let mut block_started = None;

    while let Some(Ok(update)) = stream.next().await {
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Slot(slot) => {
                log::info!(
                    "received slot update for slot {} with status {:?}",
                    slot.slot,
                    slot.status()
                );
                if slot.status() == SlotStatus::SlotCreatedBank {
                    log::info!("starting to buffer updates for slot {}", slot.slot);
                    block_started = Some(BlockBuffer {
                        slot: slot.slot,
                        account: vec![],
                        transaction: vec![],
                        entry: vec![],
                        block: None,
                        blockmeta: None,
                    });
                }
            }
            UpdateOneof::Account(ev) => {
                if let Some(block) = &mut block_started {
                    log::info!("received account update for slot {}", ev.slot);
                    ensure!(
                        block.slot == ev.slot,
                        "account update slot should match current block slot"
                    );
                    block.account.push(ev);
                }
            }
            UpdateOneof::Transaction(ev) => {
                if let Some(block) = &mut block_started {
                    log::info!("received transaction update for slot {}", ev.slot);
                    ensure!(
                        block.slot == ev.slot,
                        "transaction slot should match current block slot"
                    );
                    block.transaction.push(ev);
                }
            }
            UpdateOneof::Entry(ev) => {
                if let Some(block) = &mut block_started {
                    log::info!("received entry update for slot {}", ev.slot);
                    ensure!(
                        block.slot == ev.slot,
                        "entry slot should match current block slot"
                    );
                    block.entry.push(ev);
                }
            }
            UpdateOneof::Block(ev) => {
                if let Some(block) = &mut block_started {
                    log::info!("received block update for slot {}", ev.slot);
                    ensure!(
                        block.slot == ev.slot,
                        "block slot should match current block slot"
                    );
                    block.block = Some(ev);
                }
            }
            UpdateOneof::BlockMeta(ev) => {
                if let Some(block) = &mut block_started {
                    log::info!("received block meta update for slot {}", ev.slot);
                    ensure!(
                        block.slot == ev.slot,
                        "block meta slot should match current block slot"
                    );
                    block.blockmeta = Some(ev);
                    // We break here because block meta is the last update geyser sends for a block, so we can validate the ordering guarantees up to the end of block lifecycle.
                    break;
                }
            }
            _ => {}
        }
    }

    ensure!(
        block_started.is_some(),
        "should have received slot update indicating block start"
    );

    let block = block_started.unwrap();
    ensure!(
        block.block.is_some(),
        "should have received block update for the block"
    );
    ensure!(
        !block.account.is_empty(),
        "should have received account updates for the block"
    );
    ensure!(
        !block.transaction.is_empty(),
        "should have received transaction updates for the block"
    );
    ensure!(
        !block.entry.is_empty(),
        "should have received entry updates for the block"
    );
    ensure!(
        block.blockmeta.is_some(),
        "should have received block meta update for the block"
    );
    let blockmeta = block.blockmeta.unwrap();
    ensure!(
        blockmeta.executed_transaction_count as usize == block.transaction.len(),
        "executed transaction count in block meta should match number of transaction updates received for the block"
    );

    Ok(())
}

/// guarantees that subscribing with filters, the filters are correctly applied to account updates and the updates received match the filters specified in the subscription.
#[test_helper(name = "filter-accounts")]
pub async fn subscribe_should_filter_accounts(config: &RunConfig) -> Result<()> {
    let mut client = new_client(config).await?;

    // These pubkeys are actively used in the network and will generate account updates when transactions are processed.
    // Feel free to add more pubkeys to this list to increase the likelihood of receiving account updates during the test.
    // They are HumdiFi markets, owned by HumidiFi's program, they are one of the most updated accounts in every block, allowing us to easily verify the filter
    let accounts = vec![
        "2866MvCKPGz9LdnPcmPueoV3mA2Ac1ceEQ8Xqb9VNefu".to_string(), // PENGU-USDC
        "H3TyE2Q3rDrvRXD8PzHYE7BS2hafGuybje4qXCtyWqMH".to_string(), // HYPE-USDC
        "9c5xYTnURgpQLDk4XqkJdaUab6p8EMBgE5n7n29pQzCy".to_string(), // 2Z-USDC
        "8WFduUYU7iX94E3ZMejpTXi5TadKh9j5qp5ez5uSBJwa".to_string(), // ZEC-USDC
        "hKgG7iEDRFNsJSwLYqz8ETHuZwzh6qMMLow8VXa8pLm".to_string(),  // JUP-USDC
        "H3TyE2Q3rDrvRXD8PzHYE7BS2hafGuybje4qXCtyWqMH".to_string(), // HYPE-USDC
        "FksffEqnBRixYGR791Qw2MgdU7zNCpHVFYBL4Fa4qVuH".to_string(), // WSOL-USDC
    ];

    const HUMIDIFI_POOL_SIZE: u64 = 1728; // HumidiFi markets have a fixed account data size of 1728 bytes, we will use this to verify the datasize filter works as expected
    let owners = vec!["9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp".to_string()];

    let subscription = SubscribeRequest {
        accounts: HashMap::from([
            (
                "valid_filter".to_string(),
                SubscribeRequestFilterAccounts {
                    account: accounts.clone(),
                    nonempty_txn_signature: Some(true),
                    owner: owners.clone(),
                    filters: vec![SubscribeRequestFilterAccountsFilter {
                        filter: Some(Filter::Datasize(HUMIDIFI_POOL_SIZE)),
                    }],
                    ..Default::default()
                },
            ),
            (
                "invalid_filter_1".to_string(),
                SubscribeRequestFilterAccounts {
                    account: accounts.clone(),
                    nonempty_txn_signature: Some(true),
                    owner: owners.clone(),
                    filters: vec![SubscribeRequestFilterAccountsFilter {
                        filter: Some(Filter::Datasize(HUMIDIFI_POOL_SIZE + 1)), // HumidiFi markets have a fixed account data size of 1728 bytes, this filter should never match any updates
                    }],
                    ..Default::default()
                },
            ),
            (
                "invalid_filter_2".to_string(),
                SubscribeRequestFilterAccounts {
                    account: accounts.clone(),
                    nonempty_txn_signature: Some(false), // HumidiFi updates are not system programs therefore require a nonempty txn signature, this filter should never match any updates
                    owner: owners.clone(),
                    filters: vec![SubscribeRequestFilterAccountsFilter {
                        filter: Some(Filter::Datasize(HUMIDIFI_POOL_SIZE)),
                    }],
                    ..Default::default()
                },
            ),
            (
                "invalid_filter_3".to_string(),
                SubscribeRequestFilterAccounts {
                    account: accounts.clone(),
                    nonempty_txn_signature: Some(true),
                    owner: vec!["11111111111111111111111111111111".to_string()], // HumidiFi market accounts are not owned by the system program, this filter should never match any updates
                    filters: vec![SubscribeRequestFilterAccountsFilter {
                        filter: Some(Filter::Datasize(HUMIDIFI_POOL_SIZE)),
                    }],
                    ..Default::default()
                },
            ),
        ]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .context("subscription should succeed")?;
    const MAX_UPDATES: usize = 15;

    let mut count = 0;
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }
        let update = update.context("stream should yield updates without error")?;
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Account(account) => {
                if update.filters.is_empty() {
                    bail!("account update should have filters applied");
                }

                if !update.filters.iter().all(|f| f == "valid_filter") {
                    bail!(
                        "account update received a filter which should never come through {:?}",
                        update.filters
                    );
                }

                let slot = account.slot;

                let Some(account) = &account.account else {
                    bail!("account update should have account field");
                };

                let Ok(pubkey) = Pubkey::try_from(account.pubkey.as_slice()) else {
                    bail!("invalid account pubkey bytes");
                };

                let Ok(owner) = Pubkey::try_from(account.owner.as_slice()) else {
                    bail!("invalid account owner pubkey bytes");
                };

                let Some(signature_bytes) = &account.txn_signature else {
                    bail!("account update should have txn signature when nonempty_txn_signature filter is applied");
                };

                let Ok(signature) = Signature::try_from(signature_bytes.as_slice()) else {
                    bail!("invalid nonempty txn signature bytes");
                };

                ensure!(
                    accounts.contains(&pubkey.to_string()),
                    "received unexpected account update for pubkey {}",
                    pubkey
                );
                ensure!(
                    owners.contains(&owner.to_string()),
                    "received unexpected account update for owner {}",
                    owner
                );

                count += 1;
                log::info!(
                    "received account update for slot {slot}, signature {signature}, {count}/{MAX_UPDATES}"
                );
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => continue,
            _ => continue,
        }
    }

    Ok(())
}
