use {
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        env,
        path::{Path, PathBuf},
        str::FromStr,
        sync::Once,
    },
    tokio_stream::StreamExt,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::geyser::{
        SlotStatus, SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, TokenAccountExpansionControlFlag, subscribe_update::UpdateOneof, subscribe_update_deshred
    },
};

static LOG_INIT: Once = Once::new();

fn init_log() {
    LOG_INIT.call_once(|| {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .is_test(true) // useful in tests
            .try_init()
            .ok(); // ignore "already initialized"
    });
}

fn find_dotenv_path() -> Option<PathBuf> {
    let current = Path::new(env!("CARGO_MANIFEST_DIR"));
    let candidates = [current.join(".env")];

    candidates.into_iter().find(|path| path.exists())
}

async fn new_client() -> GeyserGrpcClient {
    if let Some(path) = find_dotenv_path() {
        dotenvy::from_path(path).expect("failed to load .dotenv file");
    }

    let endpoint = env::var("TEST_ENDPOINT")
		.or_else(|_| env::var("YELLOWSTONE_GRPC_ENDPOINT"))
		.expect("set TEST_ENDPOINT or YELLOWSTONE_GRPC_ENDPOINT (for example in yellowstone-grpc-intg-test/.dotenv)");

    let token = env::var("TEST_X_TOKEN")
        .or_else(|_| env::var("TEST_TOKEN"))
        .or_else(|_| env::var("YELLOWSTONE_GRPC_X_TOKEN"))
        .ok();

    let builder =
        GeyserGrpcClient::build_from_shared(endpoint).expect("endpoint should be a valid URI");
    let builder = builder
        .x_token(token)
        .expect("x-token should be valid ASCII metadata if provided");

    builder
        .max_decoding_message_size(100_000_000) // 100 MB, larger than the default of 4 MB, to allow for blocks with many transactions and accounts. Adjust as needed for your tests.
        .http2_adaptive_window(true)
        .accept_compressed(yellowstone_grpc_proto::tonic::codec::CompressionEncoding::Zstd)
        .connect()
        .await
        .expect("client should build from endpoint and token")
}

#[tokio::test]
async fn subscribe_should_only_returns_sysvarclock_account() {
    let mut client = new_client().await;
    let sysvar_clock_str = "SysvarC1ock11111111111111111111111111111111";
    let sysvar_clock_pubkey = Pubkey::from_str(sysvar_clock_str).expect("valid pubkey string");
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
        .expect("subscription should succeed");
    let mut count = 0usize;
    const MAX_UPDATES: usize = 3;
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }
        let update = update.expect("stream should yield updates without error");
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Account(subscribe_update_account) => {
                let account = subscribe_update_account
                    .account
                    .expect("account update should have account field");
                let actual_pubkey = account.pubkey.clone();
                let actual_pubkey = Pubkey::try_from(actual_pubkey).expect("pubkey");
                assert_eq!(actual_pubkey, sysvar_clock_pubkey);
                count += 1;
                log::info!(
                    "received account update for slot {} {count}/{MAX_UPDATES}",
                    subscribe_update_account.slot
                );
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => {
                continue;
            }
            other => {
                panic!("unexpected update type: {:?}", other);
            }
        };
    }
}

#[tokio::test]
async fn subscribe_should_receive_block_where_sysvarclock1111_account_has_been_updated() {
    init_log();
    let mut client = new_client().await;
    let sysvar_clock_str = "SysvarC1ock11111111111111111111111111111111";
    let sysvar_clock_pubkey = Pubkey::from_str(sysvar_clock_str).expect("valid pubkey string");
    let block_filter = SubscribeRequestFilterBlocks {
        account_include: vec![sysvar_clock_str.to_string()],
        ..Default::default()
    };
    let subscription = SubscribeRequest {
        blocks: HashMap::from([("test".to_string(), block_filter)]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        commitment: Some(2), // CommitmentLevel::Confirmed as i32
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .expect("subscription should succeed");
    let mut count = 0usize;
    const MAX_UPDATES: usize = 3;

    let mut block_received = HashMap::new();
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }
        let update = update.expect("stream should yield updates without error");
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Block(block) => {
                log::info!("received block update for slot {}", block.slot);
                block.accounts.iter().for_each(|account| {
                    let actual_pubkey = Pubkey::try_from(account.pubkey.clone()).expect("pubkey");
                    assert_eq!(actual_pubkey, sysvar_clock_pubkey);
                });

                // assert!(block.transactions.is_empty(), "should not receive transactions for blocks with only sysvar accounts");
                // assert!(block.entries.is_empty(), "should not receive entries for blocks with only sysvar accounts");
                let blockhash = block.blockhash.clone();
                if let Some(blockmeta_blockhash) =
                    block_received.insert(block.slot, block.blockhash)
                {
                    assert_eq!(
                        blockhash, blockmeta_blockhash,
                        "blockhash in block should match block meta update"
                    );
                    count += 1;
                }
            }
            UpdateOneof::BlockMeta(meta) => {
                log::info!("received block meta update for slot {}", meta.slot);
                let blockhash = meta.blockhash.clone();
                if let Some(block_blockhash) = block_received.insert(meta.slot, meta.blockhash) {
                    assert_eq!(
                        blockhash, block_blockhash,
                        "blockhash in meta should match block update"
                    );
                    count += 1;
                }
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => {
                continue;
            }
            other => {
                panic!("unexpected update type: {:?}", other);
            }
        };
    }
}

#[tokio::test]
async fn subscribe_should_receive_full_blocks() {
    init_log();
    let mut client = new_client().await;

    let block_filter = SubscribeRequestFilterBlocks {
        include_accounts: Some(true),
        include_transactions: Some(true),
        include_entries: Some(true),
        ..Default::default()
    };
    let subscription = SubscribeRequest {
        blocks: HashMap::from([("test".to_string(), block_filter)]),
        blocks_meta: HashMap::from([("test".to_string(), Default::default())]),
        commitment: Some(1), // CommitmentLevel::Confirmed as i32
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .expect("subscription should succeed");
    const MAX_UPDATES: usize = 12;

    let mut block_received: HashMap<u64, (String, usize)> = HashMap::new();
    let mut block_meta_received: HashMap<u64, (String, u64)> = HashMap::new();
    let mut count = 0;
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }
        let update = update.expect("stream should yield updates without error");
        let Some(update_oneof) = update.update_oneof else {
            continue;
        };

        match update_oneof {
            UpdateOneof::Block(block) => {
                assert!(
                    !block.accounts.is_empty(),
                    "should receive accounts for blocks"
                );
                assert!(
                    !block.transactions.is_empty(),
                    "should receive transactions for blocks"
                );
                assert!(
                    !block.entries.is_empty(),
                    "should receive entries for blocks"
                );
                assert_eq!(
                    block.executed_transaction_count,
                    block.transactions.len() as u64,
                    "executed transaction count should match number of transactions"
                );
                let blockhash = block.blockhash.clone();
                block_received.insert(block.slot, (blockhash.clone(), block.transactions.len()));
                if let Some((blockmeta_blockhash, actual_txn_cnt)) = block_meta_received.get(&block.slot) {
                    assert_eq!(
                        blockhash.as_str(), blockmeta_blockhash.as_str(),
                        "blockhash in block should match block meta update"
                    );
                    let txn_count = block.transactions.len();
                    assert_eq!(
                        *actual_txn_cnt, txn_count as u64,
                        "executed transaction count in meta should match number of transactions in block"
                    );
                    let account_count = block.accounts.len();
                    count += 1;
                    log::info!(
                        "received block update for slot {}, txn: {txn_count}, acct:{account_count}  {}/{MAX_UPDATES}",
                        block.slot,
                        block_received.len(),
                    );
                }
            }
            UpdateOneof::BlockMeta(meta) => {
                let blockhash = meta.blockhash.clone();
                block_meta_received.insert(meta.slot, (blockhash.clone(), meta.executed_transaction_count));

                if let Some((block_blockhash, _actual_txn_cnt)) = block_received.get(&meta.slot) {
                    assert_eq!(
                        blockhash, *block_blockhash,
                        "blockhash in meta should match block update"
                    );
                    log::info!(
                        "received block update for slot {} {count}/{MAX_UPDATES}",
                        meta.slot
                    );
                }
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => {
                continue;
            }
            other => {
                panic!("unexpected update type: {:?}", other);
            }
        };
    }
}

#[tokio::test]
async fn it_should_support_replay() {
    init_log();
    let mut client = new_client().await;

    let resp = client.get_slot(None).await.expect("get_slot");
    let tip = resp.slot;
    let sysvar_clock_str = "SysvarC1ock11111111111111111111111111111111";
    let sysvar_clock_pubkey = Pubkey::from_str(sysvar_clock_str).expect("valid pubkey string");
    let account_filter = SubscribeRequestFilterAccounts {
        account: vec![sysvar_clock_str.to_string()],

        ..Default::default()
    };
    let from_slot = tip.saturating_sub(10); // replay the last 10 slots to find some updates for the sysvar clock account
    let subscription = SubscribeRequest {
        slots: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]),
        accounts: HashMap::from([("test".to_string(), account_filter)]),
        from_slot: Some(from_slot), // replay the last 10 slots to find some updates for the sysvar clock account
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .expect("subscription should succeed");
    let mut count = 0usize;
    const MAX_UPDATES: usize = 10;
    log::info!(
        "current tip slot is {}, subscribing from slot {}",
        tip,
        from_slot
    );
    let mut remaining_slot_to_visit = Vec::from_iter((from_slot)..tip); // track the slots we expect to see in the replay
    let mut slot_status_received = HashMap::new(); // track the status of each slot we see in the replay
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }
        let update = update.expect("stream should yield updates without error");
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
                    .expect("account update should have account field");
                let actual_pubkey = account.pubkey.clone();
                let actual_pubkey = Pubkey::try_from(actual_pubkey).expect("pubkey");
                assert_eq!(actual_pubkey, sysvar_clock_pubkey);
                count += 1;
                remaining_slot_to_visit.retain(|&slot| slot != subscribe_update_account.slot);
                log::info!(
                    "received account update for slot {} {count}/{MAX_UPDATES}",
                    subscribe_update_account.slot
                );
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => {
                continue;
            }
            other => {
                panic!("unexpected update type: {:?}", other);
            }
        };
    }
    assert_eq!(
        slot_status_received.len(),
        (tip - from_slot) as usize,
        "should receive slot status updates for all slots in the replay"
    );
    assert!(
        remaining_slot_to_visit.is_empty(),
        "should have received updates for all expected slots in the replay"
    );
}

#[tokio::test]
async fn test_subscribe_deshred() {
    // For this test, we just want to verify that we receive deshred-transaction and eventually see all slot status.
    init_log();
    let mut client = new_client().await;

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
        .expect("subscription should succeed");

    let mut deshred_txn_count = 0;

    let mut remaining_slot_lifecycle_to_visit = Vec::from_iter([
        SlotStatus::SlotCompleted,
        SlotStatus::SlotConfirmed,
        SlotStatus::SlotFinalized,
        SlotStatus::SlotFirstShredReceived,
        SlotStatus::SlotCreatedBank,
        SlotStatus::SlotProcessed,
    ]); // track the slot lifecycles we expect to see in the replay

    let mut block_visit = HashSet::new();
    const BLOCK_TO_VISIT: usize = 32;
    while let Some(update) = stream.next().await {
        if block_visit.len() >= BLOCK_TO_VISIT || remaining_slot_lifecycle_to_visit.is_empty() {
            break;
        }
        let update = update.expect("stream should yield updates without error");
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
                    .expect("deshred transaction update should have transaction field");
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
        };
    }
    assert!(
        deshred_txn_count > 0,
        "should receive at least one deshred transaction update"
    );
    assert!(
        remaining_slot_lifecycle_to_visit.is_empty(),
        "should have received updates for all expected slot lifecycle in the replay. missing: {remaining_slot_lifecycle_to_visit:?}",
    );
}

#[tokio::test]
async fn any_commitment_level_of_subscription_should_return_all_possible_values() {
    init_log();
    let mut client = new_client().await;
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
            entry: HashMap::from([("test".to_string(), Default::default())]),
            transactions: HashMap::from([("test".to_string(), Default::default())]),
            accounts: HashMap::from([("test".to_string(), account_filter)]),
            commitment: Some(commitment), // test with different commitment levels
            ..Default::default()
        };

        let mut stream = client
            .subscribe_once(subscription)
            .await
            .expect("subscription should succeed");

        let mut remaining_slot_lifecycle_to_visit = Vec::from_iter([
            SlotStatus::SlotCompleted,
            SlotStatus::SlotConfirmed,
            SlotStatus::SlotFinalized,
            SlotStatus::SlotFirstShredReceived,
            SlotStatus::SlotCreatedBank,
            SlotStatus::SlotProcessed,
        ]); // track the slot lifecycles we expect to see in the replay

        let mut block_visited = HashSet::new();
        const MAX_UPDATES: usize = 32;
        let mut received_account_update = false;
        let mut rececived_txn_update = false;
        let mut received_entry = false;

        while let Some(update) = stream.next().await {
            if block_visited.len() >= MAX_UPDATES
                || (remaining_slot_lifecycle_to_visit.is_empty()
                    && received_account_update
                    && rececived_txn_update
                    && received_entry)
            {
                break;
            }
            let update = update.expect("stream should yield updates without error");
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
                _ => {}
            };
        }
        assert!(
            remaining_slot_lifecycle_to_visit.is_empty(),
            "should have received updates for all expected slot lifecycle in the replay for commitment level {commitment}. missing: {remaining_slot_lifecycle_to_visit:?}",
        );
        assert!(
            received_account_update,
            "should receive account update for commitment level {commitment}"
        );
        assert!(
            rececived_txn_update,
            "should receive transaction update for commitment level {commitment}"
        );
        assert!(
            received_entry,
            "should receive entry update for commitment level {commitment}"
        );
    }
}

#[tokio::test]
async fn it_should_subscribe_to_all_transaction_include_token_ata_to_an_owner() {
    init_log();
    let mut client = new_client().await;
    const TOKEN_ACCOUNT_OWNER: &str = "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV";
    let bisonfi_token_owner = Pubkey::from_str(TOKEN_ACCOUNT_OWNER).expect("valid pubkey string");

    let subscription = SubscribeRequest {
        transactions: HashMap::from([(
            "test".to_string(),
            SubscribeRequestFilterTransactions {
                account_include: vec![TOKEN_ACCOUNT_OWNER.to_string()],
                token_accounts: Some(TokenAccountExpansionControlFlag::BalanceChanged as i32),
                ..Default::default()
            },
        )]),
        commitment: Some(0), // CommitmentLevel::Confirmed as i32
        ..Default::default()
    };

    let mut stream = client
        .subscribe_once(subscription)
        .await
        .expect("subscription should succeed");

    let mut count = 0usize;
    const MAX_UPDATES: usize = 1;
    while let Some(update) = stream.next().await {
        if count >= MAX_UPDATES {
            break;
        }

        let update = update.expect("stream should yield updates without error");
        match update.update_oneof.unwrap() {
            UpdateOneof::Transaction(subscribe_update_transaction) => {
                let transaction = subscribe_update_transaction
                    .transaction
                    .expect("transaction update should have transaction field");

                let meta = transaction.meta.unwrap();

                let in_post_balance = meta.post_token_balances.iter().any(|b| {
                    let actual_pubkey = Pubkey::from_str(&b.owner)
                        .expect("pubkey in post balance");
                    actual_pubkey == bisonfi_token_owner
                });
                let in_pre_balance = meta.pre_token_balances.iter().any(|b| {
                    let actual_pubkey = Pubkey::from_str(&b.owner)
                        .expect("pubkey in pre balance");
                    actual_pubkey == bisonfi_token_owner
                });

                if in_post_balance && in_pre_balance {
                    log::info!("received transaction update with token balance change for account {TOKEN_ACCOUNT_OWNER}");
                    count += 1;
                } else {
                    log::info!("received transaction update but it does not have token balance change for account {TOKEN_ACCOUNT_OWNER}, pre balance has account: {in_pre_balance}, post balance has account: {in_post_balance}");
                }
            }
            _ => {}
        };
    }
}