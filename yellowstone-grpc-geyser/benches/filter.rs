use {
    bytes::Bytes,
    criterion::{criterion_group, criterion_main, BenchmarkId, Criterion},
    prost_types::Timestamp,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_message::{Message as SolMessage, MessageHeader},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    solana_transaction::{versioned::VersionedTransaction, Transaction},
    solana_transaction_status::TransactionStatusMeta,
    std::{
        collections::HashMap,
        hint::black_box,
        sync::{Arc, OnceLock},
        time::{Duration, SystemTime},
    },
    yellowstone_grpc_geyser::plugin::{
        convert_to,
        filter::{limits::FilterLimits, name::FilterNames, Filter},
        message::{
            Message, MessageAccount, MessageAccountInfo, MessageBlock, MessageBlockMeta,
            MessageEntry, MessageTransaction, MessageTransactionInfo,
        },
    },
    yellowstone_grpc_proto::geyser::{
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdateBlockMeta,
    },
};

fn filter_names() -> FilterNames {
    FilterNames::new(64, 1024, Duration::from_secs(1))
}

fn create_message_account(pubkey: Pubkey, owner: Pubkey) -> Arc<MessageAccount> {
    Arc::new(MessageAccount {
        account: MessageAccountInfo {
            pubkey,
            lamports: 1000,
            owner,
            executable: false,
            rent_epoch: 0,
            data: Bytes::new(),
            write_version: 1,
            txn_signature: None,
            pre_encoded: OnceLock::new(),
        },
        slot: 100,
        is_startup: false,
        created_at: Timestamp::from(SystemTime::now()),
    })
}

fn create_message_transaction(
    keypair: &Keypair,
    account_keys: Vec<Pubkey>,
) -> Arc<MessageTransaction> {
    let message = SolMessage {
        header: MessageHeader {
            num_required_signatures: 1,
            ..MessageHeader::default()
        },
        account_keys,
        ..SolMessage::default()
    };
    let versioned_transaction =
        VersionedTransaction::from(Transaction::new(&[keypair], message, Hash::default()));
    let meta = convert_to::create_transaction_meta(&TransactionStatusMeta {
        status: Ok(()),
        ..TransactionStatusMeta::default()
    });
    let sig = *versioned_transaction
        .signatures
        .first()
        .expect("no signature");
    let account_keys = versioned_transaction
        .message
        .static_account_keys()
        .iter()
        .copied()
        .collect();

    Arc::new(MessageTransaction {
        transaction: MessageTransactionInfo {
            signature: sig,
            is_vote: false,
            transaction: convert_to::create_transaction(&versioned_transaction),
            meta,
            index: 1,
            account_keys,
            pre_encoded: OnceLock::new(),
            token_owners_all: OnceLock::new(),
            token_owners_changed: OnceLock::new(),
        },
        slot: 100,
        created_at: Timestamp::from(SystemTime::now()),
    })
}

fn create_message_block(tx_count: usize) -> Arc<MessageBlock> {
    let keypair = Keypair::new();

    let transactions: Vec<Arc<MessageTransaction>> = (0..tx_count)
        .map(|_| {
            let mut keys = vec![keypair.pubkey()];
            keys.extend((0..29).map(|_| Pubkey::new_unique()));
            create_message_transaction(&keypair, keys)
        })
        .collect();

    let entries: Vec<Arc<MessageEntry>> = (0..64)
        .map(|i| {
            Arc::new(MessageEntry {
                slot: 100,
                index: i,
                num_hashes: 1,
                hash: Hash::default(),
                executed_transaction_count: 1,
                starting_transaction_index: i as u64,
                created_at: Timestamp::from(SystemTime::now()),
            })
        })
        .collect();

    let meta = Arc::new(MessageBlockMeta {
        block_meta: SubscribeUpdateBlockMeta {
            slot: 100,
            executed_transaction_count: tx_count as u64,
            entries_count: entries.len() as u64,
            ..Default::default()
        },
        created_at: Timestamp::from(SystemTime::now()),
    });

    Arc::new(MessageBlock::new(meta, transactions, vec![], entries))
}

fn slots_only_client(c: &mut Criterion) {
    let mut slots = HashMap::new();
    slots.insert("s".to_owned(), SubscribeRequestFilterSlots::default());

    let filter = Filter::new(
        &SubscribeRequest {
            slots,
            ..Default::default()
        },
        &FilterLimits::default(),
        &mut filter_names(),
    )
    .unwrap();

    let message = Message::Account(create_message_account(
        Pubkey::new_unique(),
        Pubkey::new_unique(),
    ));

    c.bench_function("slots_only/account", |b| {
        b.iter(|| black_box(filter.get_updates(black_box(&message), None)))
    });
}

/// Control: client with one matching account filter. The early return must
/// not cost anything here.
fn one_account_filter(c: &mut Criterion) {
    let owner = Pubkey::new_unique();

    let mut accounts = HashMap::new();
    accounts.insert(
        "a".to_owned(),
        SubscribeRequestFilterAccounts {
            owner: vec![owner.to_string()],
            ..Default::default()
        },
    );

    let filter = Filter::new(
        &SubscribeRequest {
            accounts,
            ..Default::default()
        },
        &FilterLimits::default(),
        &mut filter_names(),
    )
    .unwrap();

    let message = Message::Account(create_message_account(Pubkey::new_unique(), owner));

    c.bench_function("one_account_filter/account", |b| {
        b.iter(|| black_box(filter.get_updates(black_box(&message), None)))
    });
}

fn sweep_account_include(c: &mut Criterion) {
    let keypair = Keypair::new();
    let mut tx_keys = vec![keypair.pubkey()];
    tx_keys.extend((0..29).map(|_| Pubkey::new_unique()));
    let message = Message::Transaction(create_message_transaction(&keypair, tx_keys));

    let mut group = c.benchmark_group("sweep/account_include");
    for n in [1usize, 10, 100, 1_000, 10_000] {
        let include: Vec<String> = (0..n).map(|_| Pubkey::new_unique().to_string()).collect();

        let mut transactions = HashMap::new();
        transactions.insert(
            "t".to_owned(),
            SubscribeRequestFilterTransactions {
                account_include: include,
                ..Default::default()
            },
        );

        let mut limits = FilterLimits::default();
        limits.transactions.account_include_max = usize::MAX;

        let filter = Filter::new(
            &SubscribeRequest {
                transactions,
                ..Default::default()
            },
            &limits,
            &mut filter_names(),
        )
        .unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(filter.get_updates(black_box(&message), None)))
        });
    }
    group.finish();
}

/// Small include list where the transaction DOES touch one of the pubkeys.
/// Short-circuits on the hit.
fn small_include_matching(c: &mut Criterion) {
    let keypair = Keypair::new();
    let mut tx_keys = vec![keypair.pubkey()];
    tx_keys.extend((0..29).map(|_| Pubkey::new_unique()));
    let hit = tx_keys[15];
    let message = Message::Transaction(create_message_transaction(&keypair, tx_keys));

    let mut transactions = HashMap::new();
    transactions.insert(
        "t".to_owned(),
        SubscribeRequestFilterTransactions {
            account_include: vec![hit.to_string()],
            ..Default::default()
        },
    );

    let filter = Filter::new(
        &SubscribeRequest {
            transactions,
            ..Default::default()
        },
        &FilterLimits::default(),
        &mut filter_names(),
    )
    .unwrap();

    c.bench_function("small_include/matching", |b| {
        b.iter(|| black_box(filter.get_updates(black_box(&message), None)))
    });
}

/// N block filters, each with no account_include so every transaction
/// matches. Measures the per-filter re-walk of the whole block.
fn sweep_block_filters(c: &mut Criterion) {
    let message = Message::Block(create_message_block(1500));

    let mut group = c.benchmark_group("sweep/block_filters");
    for n in [1usize, 2, 4] {
        let mut blocks = HashMap::new();
        for i in 0..n {
            blocks.insert(
                format!("b{i}"),
                SubscribeRequestFilterBlocks {
                    include_transactions: Some(true),
                    include_entries: Some(true),
                    ..Default::default()
                },
            );
        }

        let filter = Filter::new(
            &SubscribeRequest {
                blocks,
                ..Default::default()
            },
            &FilterLimits::default(),
            &mut filter_names(),
        )
        .unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(filter.get_updates(black_box(&message), None)))
        });
    }
    group.finish();
}

/// Same block, but the filter has an account_include that matches nothing,
/// so no transactions get cloned into the output. Isolates match cost from
/// clone cost.
fn block_filter_no_matches(c: &mut Criterion) {
    let message = Message::Block(create_message_block(1500));

    let include: Vec<String> = (0..100).map(|_| Pubkey::new_unique().to_string()).collect();

    let mut blocks = HashMap::new();
    blocks.insert(
        "b".to_owned(),
        SubscribeRequestFilterBlocks {
            account_include: include,
            include_transactions: Some(true),
            include_entries: Some(true),
            ..Default::default()
        },
    );

    let filter = Filter::new(
        &SubscribeRequest {
            blocks,
            ..Default::default()
        },
        &FilterLimits::default(),
        &mut filter_names(),
    )
    .unwrap();

    c.bench_function("block_filter/no_matches", |b| {
        b.iter(|| black_box(filter.get_updates(black_box(&message), None)))
    });
}

fn block_filter_small_block(c: &mut Criterion) {
    let message = Message::Block(create_message_block(100));

    let include: Vec<String> = (0..100).map(|_| Pubkey::new_unique().to_string()).collect();

    let mut blocks = HashMap::new();
    blocks.insert(
        "b".to_owned(),
        SubscribeRequestFilterBlocks {
            account_include: include,
            include_transactions: Some(true),
            include_entries: Some(true),
            ..Default::default()
        },
    );

    let filter = Filter::new(
        &SubscribeRequest {
            blocks,
            ..Default::default()
        },
        &FilterLimits::default(),
        &mut filter_names(),
    )
    .unwrap();

    c.bench_function("block_filter/small_block", |b| {
        b.iter(|| black_box(filter.get_updates(black_box(&message), None)))
    });
}

fn block_filter_one_tx(c: &mut Criterion) {
    let message = Message::Block(create_message_block(1));

    let include: Vec<String> = (0..100).map(|_| Pubkey::new_unique().to_string()).collect();

    let mut blocks = HashMap::new();
    blocks.insert(
        "b".to_owned(),
        SubscribeRequestFilterBlocks {
            account_include: include,
            include_transactions: Some(true),
            include_entries: Some(true),
            ..Default::default()
        },
    );

    let filter = Filter::new(
        &SubscribeRequest {
            blocks,
            ..Default::default()
        },
        &FilterLimits::default(),
        &mut filter_names(),
    )
    .unwrap();

    c.bench_function("block_filter/one_tx", |b| {
        b.iter(|| black_box(filter.get_updates(black_box(&message), None)))
    });
}

criterion_group!(
    benches,
    slots_only_client,
    one_account_filter,
    sweep_account_include,
    small_include_matching,
    sweep_block_filters,
    block_filter_no_matches,
    block_filter_small_block,
    block_filter_one_tx,
);
criterion_main!(benches);
