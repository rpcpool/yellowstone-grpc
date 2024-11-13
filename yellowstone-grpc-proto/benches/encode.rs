use {
    criterion::{criterion_group, criterion_main, BenchmarkId, Criterion},
    prost::Message as _,
    std::{sync::Arc, time::Duration},
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
            SubscribeUpdateTransaction,
        },
        plugin::{
            filter::FilterAccountsDataSlice,
            message::{MessageAccount, MessageTransaction, MessageTransactionInfo},
            message_ref::{
                tests::{
                    create_accounts, create_message_filters, load_predefined_blocks,
                    load_predefined_transactions,
                },
                Message, MessageFilters, MessageRef, MessageRefBlock,
            },
        },
    },
};

fn build_subscribe_update(filters: &MessageFilters, update: UpdateOneof) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: filters.iter().map(|f| f.as_ref().to_owned()).collect(),
        update_oneof: Some(update),
    }
}

fn build_subscribe_update_account(
    message: &MessageAccount,
    data_slice: &FilterAccountsDataSlice,
) -> UpdateOneof {
    UpdateOneof::Account(SubscribeUpdateAccount {
        account: Some((message.account.as_ref(), data_slice).into()),
        slot: message.slot,
        is_startup: message.is_startup,
    })
}

fn build_subscribe_transaction(transaction: &MessageTransactionInfo, slot: u64) -> UpdateOneof {
    UpdateOneof::Transaction(SubscribeUpdateTransaction {
        transaction: Some(transaction.into()),
        slot,
    })
}

fn build_subscribe_block(block: &MessageRefBlock) -> UpdateOneof {
    UpdateOneof::Block(block.into())
}

fn bench_account(c: &mut Criterion) {
    let filters = create_message_filters(&["my special filter"]);

    let accounts = create_accounts();
    c.bench_with_input(
        BenchmarkId::new("accounts", "ref"),
        &(&accounts, &filters),
        |b, (accounts, filters)| {
            b.iter(|| {
                for (account, data_slice) in accounts.iter() {
                    let msg = Message {
                        filters: (*filters).clone(),
                        message: MessageRef::account(account, data_slice.clone()),
                    };
                    msg.encode_to_vec().len();
                }
            })
        },
    );
    c.bench_with_input(
        BenchmarkId::new("accounts", "prost"),
        &(&accounts, &filters),
        |b, (accounts, filters)| {
            b.iter(|| {
                for (account, data_slice) in accounts.iter() {
                    let msg = build_subscribe_update(
                        filters,
                        build_subscribe_update_account(account, data_slice),
                    );
                    msg.encode_to_vec().len();
                }
            })
        },
    );

    let accounts = accounts
        .iter()
        .map(|(account, data_slice)| build_subscribe_update_account(account, data_slice))
        .collect::<Vec<_>>();
    c.bench_with_input(
        BenchmarkId::new("accounts", "prost clone"),
        &(&accounts, &filters),
        |b, (accounts, filters)| {
            b.iter(|| {
                for account in accounts.iter() {
                    let msg = build_subscribe_update(filters, account.clone());
                    msg.encode_to_vec().len();
                }
            })
        },
    );

    let transactions = load_predefined_transactions();
    c.bench_with_input(
        BenchmarkId::new("transactions", "ref"),
        &(&transactions, &filters),
        |b, (transactions, filters)| {
            b.iter(|| {
                for transaction in transactions.iter() {
                    let msg = Message {
                        filters: (*filters).clone(),
                        message: MessageRef::transaction(&MessageTransaction {
                            transaction: Arc::clone(transaction),
                            slot: 42,
                        }),
                    };
                    msg.encode_to_vec().len();
                }
            })
        },
    );
    c.bench_with_input(
        BenchmarkId::new("transactions", "prost"),
        &(&transactions, &filters),
        |b, (transactions, filters)| {
            b.iter(|| {
                for transaction in transactions.iter() {
                    let msg = build_subscribe_update(
                        filters,
                        build_subscribe_transaction(transaction, 42),
                    );
                    msg.encode_to_vec().len();
                }
            })
        },
    );

    let transactions = transactions
        .into_iter()
        .map(|transaction| build_subscribe_transaction(transaction.as_ref(), 42))
        .collect::<Vec<_>>();
    c.bench_with_input(
        BenchmarkId::new("transactions", "prost clone"),
        &(&transactions, &filters),
        |b, (transactions, filters)| {
            b.iter(|| {
                for transaction in transactions.iter() {
                    let msg = build_subscribe_update(filters, transaction.clone());
                    msg.encode_to_vec().len();
                }
            })
        },
    );

    let blocks = load_predefined_blocks();
    c.bench_with_input(
        BenchmarkId::new("blocks", "ref"),
        &(blocks.as_slice(), &filters),
        |b, (blocks, filters)| {
            b.iter(|| {
                for block in blocks.iter() {
                    let msg = Message {
                        filters: (*filters).clone(),
                        message: MessageRef::block(Box::new(block.clone())),
                    };
                    msg.encode_to_vec().len();
                }
            })
        },
    );
    c.bench_with_input(
        BenchmarkId::new("blocks", "prost"),
        &(blocks.as_slice(), &filters),
        |b, (blocks, filters)| {
            b.iter(|| {
                for block in blocks.iter() {
                    let msg = build_subscribe_update(filters, build_subscribe_block(block));
                    msg.encode_to_vec().len();
                }
            })
        },
    );

    let blocks = blocks.iter().map(build_subscribe_block).collect::<Vec<_>>();
    c.bench_with_input(
        BenchmarkId::new("blocks", "prost clone"),
        &(blocks.as_slice(), &filters),
        |b, (blocks, filters)| {
            b.iter(|| {
                for block in blocks.iter() {
                    let msg = build_subscribe_update(filters, block.clone());
                    msg.encode_to_vec().len();
                }
            })
        },
    );
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3)) // default 3
        .measurement_time(Duration::from_secs(5)); // default 5
    targets = bench_account
);
criterion_main!(benches);
